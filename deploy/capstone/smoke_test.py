#!/usr/bin/env python3
"""End-to-end smoke test of a DEPLOYED Smart Weather Alert Platform (Docker Compose or AWS).

From your laptop it plays every external role of example 24 against a remote host:

1. edge devices  -> 4 sensors stream readings over gRPC (client streaming) to the ingest service;
2. dashboard     -> subscribes to the SSE alert stream;
3. operator      -> waits until the consumer lag is 0 (GraphQL ``pipeline``);
4. analyst       -> GraphQL queries over the read model;
5. AI agent      -> MCP server (local subprocess) whose tools call the REMOTE GraphQL API.

Usage:
    pip install grpcio grpcio-tools httpx
    python deploy/capstone/smoke_test.py --host 127.0.0.1          # docker compose on this machine
    python deploy/capstone/smoke_test.py --host 3.91.10.20         # public IP printed by aws/deploy.py

References:
    - gRPC Python basics: https://grpc.io/docs/languages/python/basics/
    - HTTPX streaming (SSE): https://www.python-httpx.org/quickstart/#streaming-responses
"""

from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CAPSTONE = ROOT / "examples" / "24_capstone_smart_weather_platform"
sys.path.insert(0, str(ROOT))
from common.utils import banner, log, require, section, takeaway  # noqa: E402

httpx = require("httpx")
require("grpc", "grpcio grpcio-tools")

SENSORS = [("bilbao", "mild"), ("oslo", "cold"), ("madrid", "warm"), ("sevilla", "heatwave")]


def gql(api: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run a GraphQL query against the remote API."""
    r = httpx.post(f"{api}/graphql", json={"query": query, "variables": variables or {}}, timeout=10)
    body = r.json()
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    return body["data"]


def total_offsets(api: str) -> tuple[int, int]:
    """(records in the log, total consumer lag)."""
    p = gql(api, "{ pipeline { endOffset lag } }")["pipeline"]
    return sum(x["endOffset"] for x in p), sum(x["lag"] for x in p)


def dashboard(api: str, after: int, got: list[dict[str, Any]], stop: threading.Event) -> None:
    """SSE subscriber (only alerts newer than ``after``)."""
    try:
        with httpx.stream("GET", f"{api}/alerts/stream", params={"after": after}, timeout=None) as r:
            for line in r.iter_lines():
                if stop.is_set():
                    return
                if line.startswith("data: "):
                    a = json.loads(line[6:])
                    got.append(a)
                    log("dashboard", f"SSE push: {a['level']} {a['city']} {a['celsius']}°C")
    except httpx.HTTPError:
        pass


class MCPClient:
    """Minimal MCP host (stdio JSON-RPC) for the platform's MCP server."""

    def __init__(self, api: str) -> None:
        """Start ``mcp_weather_server.py`` locally, pointing at the remote API."""
        self.p = subprocess.Popen([sys.executable, str(CAPSTONE / "mcp_weather_server.py"), "--api", api],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8", bufsize=1)
        self.ids = itertools.count(1)
        self.call("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                 "clientInfo": {"name": "smoke-test", "version": "1"}})
        self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _write(self, msg: dict[str, Any]) -> None:
        assert self.p.stdin is not None
        self.p.stdin.write(json.dumps(msg) + "\n")
        self.p.stdin.flush()

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """JSON-RPC request/response."""
        rid = next(self.ids)
        self._write({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
        assert self.p.stdout is not None
        line = self.p.stdout.readline()
        if not line:
            raise RuntimeError("MCP server exited")
        return json.loads(line)["result"]

    def tool(self, name: str, **args: Any) -> Any:
        """Call a tool, decode its JSON text content."""
        return json.loads(self.call("tools/call", {"name": name, "arguments": args})["content"][0]["text"])

    def close(self) -> None:
        """Stop the server."""
        self.p.terminate()
        self.p.wait()


def main() -> None:
    """Run the end-to-end scenario against ``--host``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="public IP / DNS of the deployment (127.0.0.1 for compose)")
    ap.add_argument("--grpc-port", type=int, default=50051)
    ap.add_argument("--api-port", type=int, default=8080)
    ap.add_argument("--n", type=int, default=12, help="readings per sensor")
    a = ap.parse_args()
    api = f"http://{a.host}:{a.api_port}"
    banner(f"Smoke test of the deployed platform at {a.host}", "24")

    section("0) Health check (REST)")
    deadline = time.monotonic() + 120
    while True:
        try:
            log("operator", f"GET {api}/health -> {httpx.get(f'{api}/health', timeout=5).json()}")
            break
        except httpx.HTTPError as exc:
            if time.monotonic() > deadline:
                raise SystemExit(f"API not reachable: {exc} (security group? task still starting?)")
            time.sleep(2)
    before, _ = total_offsets(api)
    last_alert = max([x["id"] for x in gql(api, "{ alerts(limit: 1000) { id } }")["alerts"]] or [0])
    log("operator", f"log already holds {before} readings; watching alerts after id {last_alert}")

    section("1) Edge devices stream readings over gRPC; a dashboard listens via SSE")
    got: list[dict[str, Any]] = []
    stop = threading.Event()
    threading.Thread(target=dashboard, args=(api, last_alert, got, stop), daemon=True).start()
    procs = [subprocess.Popen([sys.executable, str(CAPSTONE / "sensor.py"), "--host", a.host, "--port", str(a.grpc_port),
                               "--city", c, "--profile", p, "--n", str(a.n)]) for c, p in SENSORS]
    codes = [p.wait(timeout=120) for p in procs]
    if any(codes):
        raise SystemExit(f"sensor(s) failed: exit codes {codes} (is port {a.grpc_port} open?)")

    section("2) Wait until the consumer group has processed everything (lag = 0)")
    expected = before + len(SENSORS) * a.n
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        end, lag = total_offsets(api)
        if end >= expected and lag == 0:
            break
        time.sleep(0.5)
    log("operator", f"log records = {end} (expected {expected}), lag = {lag}")
    time.sleep(1.0)  # let SSE deliver the last alerts

    section("3) Analyst: GraphQL queries")
    stations = gql(api, "{ stations { city count mean max } }")["stations"]
    for s in stations:
        log("analyst", f"{s['city']:<8} count={s['count']:>3} mean={s['mean']:>5} max={s['max']}")
    counted = sum(s["count"] for s in stations)
    log("check", f"readings counted in the read model = {counted}, records in the log = {end} "
                 f"-> {'exactly once' if counted == end else 'MISMATCH'}")

    section("4) AI agent through MCP tools (local MCP server -> remote GraphQL)")
    mcp = MCPClient(api)
    try:
        red = mcp.tool("active_alerts", level="RED")
        cities = sorted({x["city"] for x in red})
        log("agent", f"RED alerts in {cities}; stations: {[s['city'] for s in mcp.tool('list_stations')]}")
    finally:
        mcp.close()
        stop.set()

    ok = end >= expected and lag == 0 and counted == end
    log("check", f"{'PASS' if ok else 'FAIL'}: {len(SENSORS) * a.n} readings sent, lag {lag}, "
                 f"{len(got)} new alerts pushed via SSE")
    takeaway("The same platform runs unchanged on a laptop (processes), in Docker Compose and on AWS Fargate.",
             "Only the addresses change: edge devices and clients reach it through gRPC, GraphQL, SSE and MCP.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
