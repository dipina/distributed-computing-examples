"""24 · CAPSTONE: a Smart Weather Alert Platform combining many paradigms.

A small but realistic event-driven IoT system. Every box below is a SEPARATE
OS process; they only talk through the network or shared durable storage::

   sensor-bilbao ─┐                                         ┌─► analytics-0 ─┐  (consumer group,
   sensor-oslo   ─┤ gRPC client     ┌──────────────────┐    │   partitions    │   CQRS projector)
   sensor-madrid ─┼─streaming──────►│ ingest (gRPC)    │───►│   0, 2          ├──► read model (SQLite:
   sensor-sevilla─┘  (ex. 12)       │ → commit log     │    └─► analytics-1 ─┘    views + alerts +
                                    │ 3 partitions(17) │        partition 1       offsets, 1 txn)
                                    └──────────────────┘                              │
                                                                                      ▼
   dashboard  ◄── SSE alerts (13) ─────────────── api service: GraphQL (11) + SSE + /health (10)
   analyst    ◄── GraphQL queries (11) ───────────┘            ▲
   AI agent   ◄── MCP over stdio (23) ── mcp server ── GraphQL ┘

   supervisor: desired-state reconciliation loop (22) restarts anything that dies;
   a worker is killed on purpose and resumes from its committed offsets (17) without
   losing or double-counting a single reading (effectively-once via one DB transaction).

Architecture patterns on display: event-driven architecture, CQRS (commands via gRPC,
queries via GraphQL), log-based integration, consumer groups, idempotent/transactional
consumers, self-healing supervision, observability (consumer lag), AI-agent access.

Containers & cloud: see deploy/capstone/README.md (Docker Compose and AWS ECS Fargate).

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install grpcio grpcio-tools fastapi uvicorn strawberry-graphql httpx
    Each component can be started by hand (see its docstring), e.g.:
      python examples/24_capstone_smart_weather_platform/api_service.py --data /tmp/wx --port 8080
      open http://127.0.0.1:8080/graphql  and  curl -N http://127.0.0.1:8080/alerts/stream

Tutorials & references:
    - M. Fowler: CQRS
      https://martinfowler.com/bliki/CQRS.html
    - microservices.io: Transactional outbox / idempotent consumer
      https://microservices.io/patterns/data/transactional-outbox.html
    - Kafka design: delivery semantics (exactly-once)
      https://kafka.apache.org/documentation/#semantics
    - SQLite: write-ahead logging
      https://www.sqlite.org/wal.html
    - SQLite: UPSERT
      https://www.sqlite.org/lang_upsert.html
    - Kleppmann, Designing Data-Intensive Applications
      https://dataintensive.net/
    - Also: the references of examples 10-13, 17, 22 and 23

Run:       python examples/24_capstone_smart_weather_platform/demo.py
Requires:  pip install grpcio grpcio-tools fastapi uvicorn strawberry-graphql httpx
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]
from common.utils import banner, free_port, log, require, section, takeaway, wait_for_port  # noqa: E402

for mod, pip_name in [("grpc", "grpcio"), ("grpc_tools", "grpcio-tools"), ("fastapi", "fastapi"),
                      ("uvicorn", "uvicorn"), ("strawberry", "strawberry-graphql"), ("httpx", "httpx")]:
    require(mod, pip_name)
import httpx  # noqa: E402

import proto_stubs  # noqa: E402,F401 - compile the .proto once, before spawning components

PY = sys.executable
SENSORS = [("bilbao", "mild"), ("oslo", "cold"), ("madrid", "warm"), ("sevilla", "heatwave")]
READINGS_PER_SENSOR = 12


class Supervisor:
    """Keeps the declared components running (reconciliation loop, like example 22)."""

    def __init__(self) -> None:
        """Start the control loop."""
        self.desired: dict[str, list[str]] = {}
        self.procs: dict[str, subprocess.Popen[bytes]] = {}
        self.restarts: dict[str, int] = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def declare(self, name: str, argv: list[str]) -> None:
        """Add a component to the desired state and reconcile immediately."""
        with self.lock:
            self.desired[name] = argv
            self._reconcile()

    def _reconcile(self) -> None:
        for name, argv in self.desired.items():
            p = self.procs.get(name)
            if p is not None and p.poll() is None:
                continue
            if p is not None:
                self.restarts[name] = self.restarts.get(name, 0) + 1
                log("supervisor", f"{name} is down (exit {p.returncode}) -> restarting")
            self.procs[name] = subprocess.Popen(argv, env={**os.environ, "PYTHONUNBUFFERED": "1"})
            log("supervisor", f"started {name} (pid {self.procs[name].pid})")

    def _loop(self) -> None:
        while not self._stop.wait(0.2):
            with self.lock:
                self._reconcile()

    def chaos_kill(self, name: str) -> None:
        """Kill a component abruptly (SIGKILL): no cleanup, no goodbye."""
        with self.lock:
            p = self.procs[name]
            p.kill()
            p.wait()
        log("chaos", f"💥 killed {name} (pid {p.pid})")

    def shutdown(self) -> None:
        """Stop reconciling and terminate everything."""
        self._stop.set()
        with self.lock:
            self.desired.clear()
            for p in self.procs.values():
                p.terminate()
            for p in self.procs.values():
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()


def gql(api: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run a GraphQL query against the platform API."""
    r = httpx.post(f"{api}/graphql", json={"query": query, "variables": variables or {}}, timeout=5)
    body = r.json()
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    return body["data"]


def dashboard(api: str, received: list[dict[str, Any]], stop: threading.Event) -> None:
    """A live dashboard subscribed to the SSE alert stream."""
    try:
        with httpx.stream("GET", f"{api}/alerts/stream", timeout=None) as r:
            for line in r.iter_lines():
                if stop.is_set():
                    return
                if line.startswith("data: "):
                    a = json.loads(line[6:])
                    received.append(a)
                    log("dashboard", f"🔔 SSE push: {a['level']} {a['city']} {a['celsius']}°C")
    except httpx.HTTPError:
        pass


class MCPClient:
    """Minimal MCP host talking to the platform's MCP server over stdio."""

    def __init__(self, argv: list[str]) -> None:
        """Spawn the MCP server and perform the handshake."""
        self.p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        self.n = 0
        info = self.call("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                        "clientInfo": {"name": "capstone-agent", "version": "1"}})
        self.p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")  # type: ignore[union-attr]
        log("agent", f"connected to MCP server {info['serverInfo']['name']!r}")

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """JSON-RPC request/response."""
        self.n += 1
        self.p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or {}}) + "\n")  # type: ignore[union-attr]
        self.p.stdin.flush()  # type: ignore[union-attr]
        return json.loads(self.p.stdout.readline())["result"]  # type: ignore[union-attr]

    def tool(self, name: str, **args: Any) -> Any:
        """Call a tool and decode its JSON text content."""
        return json.loads(self.call("tools/call", {"name": name, "arguments": args})["content"][0]["text"])

    def close(self) -> None:
        """Stop the server."""
        self.p.terminate()
        self.p.wait()


def ai_agent(mcp: MCPClient) -> None:
    """A rule-based stand-in for an LLM agent: discover tools, gather facts, write a briefing."""
    tools = [t["name"] for t in mcp.call("tools/list")["tools"]]
    log("agent", f"discovered tools: {tools}")
    alerts = mcp.tool("active_alerts", level="RED")
    hot_cities = sorted({a["city"] for a in alerts})
    log("agent", f"active_alerts(level=RED) -> {len(alerts)} alert(s) in {hot_cities}")
    for city in hot_cities:
        rep = mcp.tool("station_report", city=city)
        log("agent", f"station_report({city}) -> max {rep['max']}°C, mean {rep['mean']}°C over {rep['count']} readings")
    stations = mcp.tool("list_stations")
    coolest = min(stations, key=lambda s: s["mean"])
    brief = (f"BRIEFING: RED heat alerts in {', '.join(hot_cities) or 'no city'}; "
             f"coolest refuge: {coolest['city']} (mean {coolest['mean']}°C).")
    log("agent", brief)


def main() -> None:
    """Deploy the platform, generate traffic, inject a failure, query it every way."""
    banner("24 · CAPSTONE: event-driven Smart Weather Alert Platform (gRPC + log + CQRS + GraphQL + SSE + MCP)",
           added=True)
    data = Path(tempfile.mkdtemp(prefix="capstone-"))
    ingest_port, api_port = free_port(), free_port()
    api = f"http://127.0.0.1:{api_port}"
    sup = Supervisor()
    stop = threading.Event()
    mcp = None
    try:
        section("1) Deploy: the supervisor starts every component as its own process (desired state)")
        sup.declare("ingest", [PY, str(HERE / "ingest_service.py"), "--data", str(data), "--port", str(ingest_port)])
        sup.declare("api", [PY, str(HERE / "api_service.py"), "--data", str(data), "--port", str(api_port)])
        for w in (0, 1):
            sup.declare(f"analytics-{w}", [PY, str(HERE / "analytics_worker.py"), "--data", str(data),
                                          "--worker", str(w), "--workers", "2"])
        wait_for_port(ingest_port, timeout=30)
        wait_for_port(api_port, timeout=30)
        log("deploy", f"health check -> {httpx.get(f'{api}/health').json()}")

        section("2) Traffic: 4 edge sensors stream readings over gRPC; a dashboard listens via SSE")
        received: list[dict[str, Any]] = []
        threading.Thread(target=dashboard, args=(api, received, stop), daemon=True).start()
        sensors = [subprocess.Popen([PY, str(HERE / "sensor.py"), "--city", c, "--profile", prof,
                                     "--port", str(ingest_port), "--n", str(READINGS_PER_SENSOR)])
                   for c, prof in SENSORS]

        time.sleep(1.4)  # let the pipeline run for a while before injecting the failure
        section("3) Chaos: kill analytics-0 mid-stream -> supervisor restarts it -> resumes from committed offsets")
        sup.chaos_kill("analytics-0")
        for s in sensors:
            s.wait(timeout=30)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            lag = gql(api, "{ pipeline { partition endOffset committed lag } }")["pipeline"]
            if sum(p["lag"] for p in lag) == 0 and sum(p["endOffset"] for p in lag) == len(SENSORS) * READINGS_PER_SENSOR:
                break
            time.sleep(0.2)
        log("observability", f"consumer lag per partition: {[(p['partition'], p['lag']) for p in lag]} -> caught up")
        time.sleep(0.5)  # let the SSE stream deliver the last alerts

        section("4) Analyst: GraphQL queries over the read model (only the fields needed, one round trip)")
        st = gql(api, "{ stations { city count mean max } }")["stations"]
        for s in st:
            log("analyst", f"{s['city']:<8} count={s['count']:>2} mean={s['mean']:>5} max={s['max']}")
        sev = gql(api, 'query($c: String!) { station(city: $c) { city last alerts(limit: 3) { level celsius } } }',
                  {"c": "sevilla"})["station"]
        log("analyst", f"station(sevilla) with nested alerts -> {sev}")

        section("5) Correctness check: nothing lost, nothing counted twice despite the crash")
        total = sum(s["count"] for s in st)
        expected = len(SENSORS) * READINGS_PER_SENSOR
        n_alerts = len(gql(api, "{ alerts(limit: 100) { id } }")["alerts"])
        log("check", f"readings in read model = {total} / sent = {expected} -> {'OK' if total == expected else 'MISMATCH'}")
        log("check", f"alerts stored = {n_alerts}, pushed to dashboard via SSE = {len(received)}; "
                     f"supervisor restarts = {sup.restarts}")
        if total != expected:
            raise SystemExit("exactly-once check failed")

        section("6) AI agent: uses the platform through MCP tools (which call the GraphQL API)")
        mcp = MCPClient([PY, str(HERE / "mcp_weather_server.py"), "--api", api])
        ai_agent(mcp)
    finally:
        stop.set()
        if mcp:
            mcp.close()
        sup.shutdown()
        shutil.rmtree(data, ignore_errors=True)

    takeaway(
        "Each paradigm does the job it is best at: gRPC for device->cloud ingestion, a partitioned log for "
        "decoupling and replay, GraphQL for flexible reads, SSE for push, MCP for AI agents.",
        "CQRS: writes enter as events (gRPC -> log), reads come from projections (GraphQL over the read model).",
        "Updating the projection and committing the offset in ONE transaction = effectively-once processing.",
        "Supervision + durable offsets turn a crash into a delay, not data loss (self-healing, like Kubernetes).",
        "Consumer lag is the key health metric of an event-driven system.",
    )


if __name__ == "__main__":
    main()
