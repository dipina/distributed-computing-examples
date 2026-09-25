"""15 · Microservices behind an API Gateway (slides 27-39).

The monolithic WeatherService is split into three **independently deployed
processes**, each with its own data and HTTP API:

    stations-svc   /stations/{city}   current readings
    forecast-svc   /forecast/{city}   (slow, and we will crash it)
    alerts-svc     /alerts/{city}     active alerts

An **API Gateway** (single entry point, Facade pattern) sits in front and does:

* **routing** (reverse proxy)       /api/stations/{city} -> stations-svc
* **composition / aggregation**     /api/dashboard/{city} calls the 3 services *in parallel*
* **authentication**                X-API-Key header
* **rate limiting**                 token bucket per key -> 429
* **caching**                       short TTL cache of dashboards
* **resilience**                    timeouts + **circuit breaker** + fallback (degraded response)
* **distributed tracing**           a ``traceparent`` id propagated to every service (W3C Trace Context,
                                    as OpenTelemetry does) -> one request can be followed across processes

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install fastapi uvicorn httpx
    optional real tracing: pip install opentelemetry-distro opentelemetry-exporter-otlp
                           opentelemetry-bootstrap -a install

Tutorials & references:
    - microservices.io: Microservice Architecture pattern
      https://microservices.io/patterns/microservices.html
    - microservices.io: API Gateway / BFF
      https://microservices.io/patterns/apigateway.html
    - microservices.io: Circuit Breaker
      https://microservices.io/patterns/reliability/circuit-breaker.html
    - M. Fowler: CircuitBreaker
      https://martinfowler.com/bliki/CircuitBreaker.html
    - W3C Trace Context (traceparent header)
      https://www.w3.org/TR/trace-context/
    - OpenTelemetry Python: getting started
      https://opentelemetry.io/docs/languages/python/getting-started/
    - NGINX as an API gateway (slide 39)
      https://dzone.com/articles/deploying-nginx-plus-as-an-api-gateway-part-1-ngin

Run:       python examples/15_microservices_gateway/demo.py
Requires:  pip install fastapi uvicorn httpx
"""

from __future__ import annotations

import argparse
import asyncio
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import UvicornThread, banner, free_port, log, require, section, takeaway, wait_for_port  # noqa: E402

require("fastapi", "fastapi uvicorn")
httpx = require("httpx")
require("uvicorn")
from fastapi import FastAPI, Header, HTTPException, Request  # noqa: E402

from common.domain import WeatherService  # noqa: E402

API_KEYS = {"student-key": "student", "prof-key": "professor"}


# ============================================================ the microservices
def create_service(name: str) -> FastAPI:
    """Build one microservice. Each owns its data (database-per-service)."""
    app = FastAPI(title=name)
    data = WeatherService()

    def trace(request: Request, msg: str) -> None:
        tid = request.headers.get("traceparent", "-").split("-")[1][:8] if "traceparent" in request.headers else "-"
        log(f"{name}", f"trace={tid} {msg}")

    if name == "stations-svc":
        @app.get("/stations/{city}")
        def station(city: str, request: Request) -> dict[str, Any]:
            trace(request, f"GET /stations/{city}")
            try:
                return data.summary(city)
            except KeyError:
                raise HTTPException(404, "unknown city") from None

    elif name == "forecast-svc":
        @app.get("/forecast/{city}")
        async def forecast(city: str, request: Request) -> dict[str, Any]:
            trace(request, f"GET /forecast/{city} (thinking...)")
            await asyncio.sleep(0.2)
            base = data.summary(city)["mean"] if city in data.cities() else 20.0
            return {"city": city, "tomorrow": round(base + 1.5, 1), "model": "persistence+1.5"}

    elif name == "alerts-svc":
        @app.get("/alerts/{city}")
        def alerts(city: str, request: Request) -> dict[str, Any]:
            trace(request, f"GET /alerts/{city}")
            return {"city": city, "alerts": ["wind"] if city == "bilbao" else []}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "up"}

    return app


# ============================================================ gateway building blocks
# See: https://martinfowler.com/bliki/CircuitBreaker.html
class CircuitBreaker:
    """CLOSED -> (N failures) -> OPEN -> (timeout) -> HALF_OPEN -> success: CLOSED / failure: OPEN."""

    def __init__(self, name: str, threshold: int = 2, reset_after: float = 1.5) -> None:
        """Configure the breaker."""
        self.name, self.threshold, self.reset_after = name, threshold, reset_after
        self.failures, self.state, self.opened_at = 0, "CLOSED", 0.0

    def allow(self) -> bool:
        """May a call go through right now?"""
        if self.state == "OPEN" and time.monotonic() - self.opened_at >= self.reset_after:
            self._set("HALF_OPEN")
        return self.state != "OPEN"

    def success(self) -> None:
        """Record a successful call."""
        self.failures = 0
        if self.state != "CLOSED":
            self._set("CLOSED")

    def failure(self) -> None:
        """Record a failed call."""
        self.failures += 1
        if self.state == "HALF_OPEN" or self.failures >= self.threshold:
            self.opened_at = time.monotonic()
            self._set("OPEN")

    def _set(self, state: str) -> None:
        log("gateway", f"circuit[{self.name}] {self.state} -> {state}")
        self.state = state


# See: https://en.wikipedia.org/wiki/Token_bucket
class TokenBucket:
    """Rate limiter: ``rate`` tokens/second, bursts up to ``capacity``."""

    def __init__(self, rate: float, capacity: int) -> None:
        """Start full."""
        self.rate, self.capacity, self.tokens, self.t = rate, capacity, float(capacity), time.monotonic()

    def take(self) -> bool:
        """Consume one token if available."""
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


def create_gateway(urls: dict[str, str]) -> FastAPI:
    """Build the API Gateway app, given the service base URLs."""
    app = FastAPI(title="api-gateway")
    breakers = {s: CircuitBreaker(s) for s in urls}
    buckets: dict[str, TokenBucket] = {}
    cache: dict[str, tuple[float, dict[str, Any]]] = {}
    client = httpx.AsyncClient(timeout=0.5)  # per-call timeout to backends

    def authenticate(key: str | None) -> str:
        if key not in API_KEYS:
            raise HTTPException(401, "missing or invalid X-API-Key")
        if not buckets.setdefault(key, TokenBucket(rate=2, capacity=5)).take():
            raise HTTPException(429, "rate limit exceeded")
        return API_KEYS[key]

    async def call(service: str, path: str, traceparent: str) -> dict[str, Any] | None:
        br = breakers[service]
        if not br.allow():
            log("gateway", f"{service}: circuit OPEN -> fail fast, no network call")
            return None
        try:
            r = await client.get(urls[service] + path, headers={"traceparent": traceparent})
            r.raise_for_status()
            br.success()
            return r.json()
        except httpx.HTTPError as exc:
            log("gateway", f"{service}: call failed ({type(exc).__name__})")
            br.failure()
            return None

    @app.get("/api/stations/{city}")
    async def route_station(city: str, request: Request, x_api_key: str | None = Header(None)) -> Any:
        """Plain routing: forward to the owning microservice."""
        authenticate(x_api_key)
        res = await call("stations", f"/stations/{city}", request.state.traceparent)
        if res is None:
            raise HTTPException(502, "stations service unavailable")
        return res

    @app.get("/api/dashboard/{city}")
    async def dashboard(city: str, request: Request, x_api_key: str | None = Header(None)) -> dict[str, Any]:
        """Composition: fan out to 3 services in parallel and aggregate (with fallbacks)."""
        user = authenticate(x_api_key)
        if city in cache and time.monotonic() - cache[city][0] < 0.5:
            return {**cache[city][1], "cache": "HIT"}
        tp = request.state.traceparent
        st, fc, al = await asyncio.gather(call("stations", f"/stations/{city}", tp),
                                          call("forecast", f"/forecast/{city}", tp),
                                          call("alerts", f"/alerts/{city}", tp))
        degraded = [n for n, v in (("stations", st), ("forecast", fc), ("alerts", al)) if v is None]
        body = {"user": user, "city": city, "now": st and st["last"],
                "tomorrow": fc["tomorrow"] if fc else "n/a (fallback)",
                "alerts": al["alerts"] if al else [], "degraded": degraded}
        if not degraded:
            cache[city] = (time.monotonic(), body)
        return {**body, "cache": "MISS"}

    @app.middleware("http")
    async def tracing(request: Request, call_next: Any) -> Any:
        """Create (or continue) a W3C trace context and time the request."""
        # See: https://www.w3.org/TR/trace-context/#traceparent-header
        tp = request.headers.get("traceparent") or f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"
        request.state.traceparent = tp
        t0 = time.perf_counter()
        resp = await call_next(request)
        log("gateway", f"trace={tp.split('-')[1][:8]} {request.url.path} -> {resp.status_code} "
                       f"in {(time.perf_counter() - t0) * 1000:.0f} ms")
        resp.headers["traceparent"] = tp
        return resp

    return app


# ============================================================ orchestration of the demo
def spawn(service: str, port: int) -> subprocess.Popen[bytes]:
    """Launch a microservice as a separate OS process."""
    p = subprocess.Popen([sys.executable, __file__, "--service", service, "--port", str(port)], env=os.environ.copy())
    wait_for_port(port, timeout=20)
    return p


def main() -> None:
    """Either run one microservice (``--service``) or the whole demo."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", choices=["stations-svc", "forecast-svc", "alerts-svc"])
    ap.add_argument("--port", type=int)
    args = ap.parse_args()
    if args.service:
        import uvicorn
        uvicorn.run(create_service(args.service), host="127.0.0.1", port=args.port, log_level="warning")
        return

    banner("15 · Microservices + API Gateway (routing, aggregation, resilience, tracing)", "27-39")
    ports = {s: free_port() for s in ("stations", "forecast", "alerts")}
    procs = {s: spawn(f"{s}-svc", p) for s, p in ports.items()}
    log("deploy", "3 microservices running as separate processes: " + ", ".join(f"{s}:{p}" for s, p in ports.items()))
    urls = {s: f"http://127.0.0.1:{p}" for s, p in ports.items()}

    try:
        with UvicornThread(create_gateway(urls)) as gw, httpx.Client(base_url=gw.url, timeout=5) as c:
            key = {"X-API-Key": "student-key"}
            def get(path: str, headers: dict[str, str] | None = None) -> None:
                r = c.get(path, headers=headers if headers is not None else key)
                log("client", f"GET {path} -> {r.status_code} {r.json()}")

            section("Authentication at the edge and simple routing")
            get("/api/stations/bilbao", headers={})
            get("/api/stations/bilbao")

            section("Composition: one client call -> 3 parallel backend calls (same trace id everywhere)")
            get("/api/dashboard/bilbao")
            get("/api/dashboard/bilbao")  # cached
            time.sleep(0.6)

            section("Failure: forecast-svc crashes -> timeouts, circuit opens, degraded responses")
            procs["forecast"].kill()
            procs["forecast"].wait()
            log("deploy", "💥 forecast-svc killed")
            for _ in range(3):
                get("/api/dashboard/madrid")

            section("Recovery: forecast-svc is redeployed; after the reset timeout the breaker half-opens")
            procs["forecast"] = spawn("forecast-svc", ports["forecast"])
            log("deploy", "forecast-svc back online")
            time.sleep(1.6)
            get("/api/dashboard/madrid")

            section("Rate limiting: a burst of requests from one client")
            time.sleep(2.5)  # refill the bucket
            codes = [c.get("/api/stations/oslo", headers=key).status_code for _ in range(8)]
            log("client", f"8 quick requests -> status codes {codes}")
    finally:
        for p in procs.values():
            p.terminate()
            p.wait()

    takeaway(
        "Each microservice is its own process with its own data; it can be deployed/scaled/fail independently.",
        "The gateway is the single entry point: auth, rate limits, routing, aggregation, caching.",
        "Partial failures are normal: timeouts + circuit breakers + fallbacks keep the system responsive.",
        "Trace context propagation (OpenTelemetry) is how you debug one request across many services.",
        "In production: Kong/NGINX/Envoy gateways, and service meshes (Istio, Linkerd) for service-to-service.",
    )


if __name__ == "__main__":
    main()
