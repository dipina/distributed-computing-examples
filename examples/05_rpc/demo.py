"""05 · Remote Procedure Call (slides 10, 20).

RPC makes a call to a function in another process *look like* a local call.
A client **stub** marshals the procedure name + arguments, sends them, waits,
and unmarshals the result. We show two RPC flavours from the slides' API timeline:

1. **XML-RPC** (1998) - Python's standard library gives both server and client stubs.
   We also print the XML actually sent on the wire.
2. **JSON-RPC 2.0** - a hand-written server/client to expose the mechanics:
   request ids, errors, notifications (no reply) and batches.

Finally we show why "remote != local" (fallacies of distributed computing):
a remote fault and a network timeout.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install zeep            # optional: SOAP client (the other RPC style of slide 20)

Tutorials & references:
    - xmlrpc.server
      https://docs.python.org/3/library/xmlrpc.server.html
    - xmlrpc.client
      https://docs.python.org/3/library/xmlrpc.client.html
    - JSON-RPC 2.0 specification
      https://www.jsonrpc.org/specification
    - http.server
      https://docs.python.org/3/library/http.server.html
    - Fallacies of distributed computing
      https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing
    - Zeep: Python SOAP client
      https://docs.python-zeep.org/

Run everything:  python examples/05_rpc/demo.py
Two terminals:   python examples/05_rpc/demo.py --role server --port 8000
                 python examples/05_rpc/demo.py --role client --port 8000
"""

from __future__ import annotations

import argparse
import itertools
import json
import socket
import sys
import time
import urllib.request
import xmlrpc.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, free_port, log, section, start_thread, takeaway  # noqa: E402

SERVICE = WeatherService()


def slow_forecast(city: str) -> str:
    """A slow remote procedure (to demonstrate client timeouts)."""
    time.sleep(3)
    return f"sunny in {city}"


# ------------------------------------------------------------------ XML-RPC
class QuietHandler(SimpleXMLRPCRequestHandler):
    """XML-RPC handler without per-request access logs."""

    def log_message(self, *args: Any) -> None:  # noqa: D102
        pass


def make_xmlrpc_server(port: int) -> SimpleXMLRPCServer:
    """Create an XML-RPC server exposing the weather service."""
    # See: https://docs.python.org/3/library/xmlrpc.server.html#simplexmlrpcserver-objects
    srv = SimpleXMLRPCServer(("127.0.0.1", port), requestHandler=QuietHandler, allow_none=True, logRequests=False)
    srv.register_introspection_functions()  # system.listMethods, system.methodHelp
    srv.register_instance(SERVICE)  # every public method becomes a remote procedure
    srv.register_function(slow_forecast)
    return srv


class LoggingTransport(xmlrpc.client.Transport):
    """Client transport that prints the XML request payload (for teaching)."""

    shown = False

    def send_request(self, host: Any, handler: Any, request_body: bytes, debug: Any) -> Any:  # noqa: D102
        if b"get_temperature" in request_body and not LoggingTransport.shown:
            LoggingTransport.shown = True
            log("wire", "XML-RPC request body:\n" + request_body.decode())
        return super().send_request(host, handler, request_body, debug)


# ------------------------------------------------------------------ JSON-RPC 2.0
# See: https://www.jsonrpc.org/specification#request_object
def jsonrpc_dispatch(req: dict[str, Any]) -> dict[str, Any] | None:
    """Execute one JSON-RPC 2.0 request object; return None for notifications."""
    rid = req.get("id")
    method = getattr(SERVICE, req.get("method", ""), None)
    if method is None or req["method"].startswith("_"):
        resp: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "Method not found"}}
    else:
        try:
            params = req.get("params", [])
            result = method(**params) if isinstance(params, dict) else method(*params)
            resp = {"jsonrpc": "2.0", "id": rid, "result": result}
        except Exception as exc:  # noqa: BLE001
            resp = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": repr(exc)}}
    return None if rid is None else resp


class JsonRpcHandler(BaseHTTPRequestHandler):
    """HTTP POST endpoint implementing JSON-RPC 2.0 (single + batch)."""

    def do_POST(self) -> None:  # noqa: N802
        """Handle a JSON-RPC call."""
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if isinstance(payload, list):
            out: Any = [r for r in map(jsonrpc_dispatch, payload) if r is not None]
        else:
            out = jsonrpc_dispatch(payload)
        body = json.dumps(out).encode() if out else b""
        self.send_response(200 if body else 204)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:  # noqa: D102
        pass


class JsonRpcProxy:
    """Client stub: ``proxy.get_temperature("bilbao")`` -> HTTP POST."""

    def __init__(self, url: str) -> None:
        """Remember the endpoint URL."""
        self._url = url
        self._ids = itertools.count(1)

    def _post(self, payload: Any) -> Any:
        req = urllib.request.Request(self._url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            raw = r.read()
        return json.loads(raw) if raw else None

    def __getattr__(self, name: str) -> Any:
        def stub(*args: Any) -> Any:
            resp = self._post({"jsonrpc": "2.0", "method": name, "params": list(args), "id": next(self._ids)})
            if "error" in resp:
                raise RuntimeError(resp["error"])
            return resp["result"]
        return stub

    def notify(self, method: str, *args: Any) -> None:
        """Fire-and-forget call (no id -> the server sends no reply)."""
        self._post({"jsonrpc": "2.0", "method": method, "params": list(args)})

    def batch(self, calls: list[tuple[str, list[Any]]]) -> Any:
        """Send several calls in one HTTP round-trip."""
        return self._post([{"jsonrpc": "2.0", "method": m, "params": p, "id": i} for i, (m, p) in enumerate(calls, 1)])


# ------------------------------------------------------------------ scenarios
def run_clients(xml_port: int, json_port: int) -> None:
    """Exercise both RPC flavours."""
    section("XML-RPC: the proxy makes remote procedures look local")
    proxy = xmlrpc.client.ServerProxy(f"http://127.0.0.1:{xml_port}", transport=LoggingTransport(), allow_none=True)
    log("xml-client", f"system.listMethods() -> {[m for m in proxy.system.listMethods() if not m.startswith('system')]}")
    log("xml-client", f"get_temperature('bilbao') -> {proxy.get_temperature('bilbao')}")
    log("xml-client", f"report('bilbao', 22.3) -> {proxy.report('bilbao', 22.3)}")

    section("JSON-RPC 2.0: same idea, JSON payloads, plus notifications and batches")
    jp = JsonRpcProxy(f"http://127.0.0.1:{json_port}")
    log("json-client", f"summary('madrid') -> {jp.summary('madrid')}")
    jp.notify("report", "madrid", 30.1)
    log("json-client", "notify report('madrid', 30.1) -> (no response by design)")
    res = jp.batch([("get_temperature", ["madrid"]), ("get_temperature", ["oslo"]), ("cities", [])])
    log("json-client", f"batch of 3 calls in ONE round trip -> {[r['result'] for r in res]}")

    section("Remote is NOT local: remote faults and network timeouts")
    try:
        proxy.get_temperature("atlantis")
    except xmlrpc.client.Fault as f:
        log("xml-client", f"remote exception propagated as Fault: {f.faultString}")
    try:
        jp.drop_database()
    except RuntimeError as e:
        log("json-client", f"JSON-RPC error object: {e}")
    socket.setdefaulttimeout(1.0)
    try:
        xmlrpc.client.ServerProxy(f"http://127.0.0.1:{xml_port}").slow_forecast("bilbao")
    except (TimeoutError, socket.timeout):
        log("xml-client", "slow_forecast timed out after 1s -> did it run or not? The client cannot know!")
    finally:
        socket.setdefaulttimeout(None)


def main() -> None:
    """Entry point supporting ``--role demo|server|client``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["demo", "server", "client"], default="demo")
    ap.add_argument("--port", type=int, default=0, help="XML-RPC port; JSON-RPC uses port+1")
    args = ap.parse_args()
    banner("05 · RPC: XML-RPC and JSON-RPC 2.0", "10, 20")

    xml_port = args.port or free_port()
    json_port = args.port + 1 if args.port else free_port()
    if args.role in {"demo", "server"}:
        xsrv = make_xmlrpc_server(xml_port)
        jsrv = ThreadingHTTPServer(("127.0.0.1", json_port), JsonRpcHandler)
        start_thread(jsrv.serve_forever)
        log("server", f"XML-RPC on :{xml_port}, JSON-RPC on :{json_port}")
        if args.role == "server":
            xsrv.serve_forever()
            return
        start_thread(xsrv.serve_forever)
    run_clients(xml_port, json_port)
    if args.role == "demo":
        takeaway(
            "RPC hides marshalling and transport behind a stub: remote calls read like local calls.",
            "It is action-oriented (verbs: get_temperature, report) - unlike REST's resources (see 10).",
            "Transparency is leaky: latency, partial failures and timeouts don't exist in local calls.",
            "After a timeout you don't know whether the call ran -> design idempotent operations.",
        )


if __name__ == "__main__":
    main()
