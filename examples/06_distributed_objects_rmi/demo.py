"""06 · Distributed objects: RMI / ORB + network (directory) service (slides 11-13).

RPC calls *procedures*; distributed-object systems (Java RMI, CORBA, DCOM,
Pyro in Python) invoke *methods on remote objects* that keep state and can be
passed around **by reference**. We build the three classic pieces:

* **Registry / naming service** (rmiregistry, CORBA Naming, Jini lookup service):
  maps a name to a remote reference. Registrations are **leases** that must be
  renewed (Jini idea): if a server dies silently, its entries expire.
* **Object server / ORB** with a *skeleton* per object: receives
  ``(object_id, method, args)`` and dispatches it to the real object.
  Returned objects that aren't plain data are *exported* and sent as references.
* **Client stub / proxy** generated dynamically with ``__getattr__``.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install Pyro5           # real Python RMI library
    python -m Pyro5.nameserver  # Pyro's naming service (the 'rmiregistry')

Tutorials & references:
    - Pyro5: intro and example
      https://pyro5.readthedocs.io/en/latest/intro.html
    - Pyro5 tutorial
      https://pyro5.readthedocs.io/en/latest/tutorials.html
    - Pyro5 name server
      https://pyro5.readthedocs.io/en/latest/nameserver.html
    - Oracle Java tutorial: RMI
      https://docs.oracle.com/javase/tutorial/rmi/
    - OMG CORBA specification
      https://www.omg.org/spec/CORBA/

Run:  python examples/06_distributed_objects_rmi/demo.py
"""

from __future__ import annotations

import json
import socket
import socketserver
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, log, section, start_thread, takeaway  # noqa: E402

PLAIN = (int, float, str, bool, type(None), list, dict)


def call(port: int, msg: dict[str, Any]) -> Any:
    """Send one JSON request to ``port`` and return the decoded reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall((json.dumps(msg) + "\n").encode())
        return json.loads(s.makefile().readline())


def serve(handler_fn: Any) -> socketserver.ThreadingTCPServer:
    """Start a threaded JSON-lines TCP server around ``handler_fn(msg) -> reply``."""
    class H(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            self.wfile.write((json.dumps(handler_fn(json.loads(self.rfile.readline()))) + "\n").encode())
    srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), H)
    srv.daemon_threads = True
    start_thread(lambda: srv.serve_forever(poll_interval=0.05))
    return srv


# ======================================================== naming service
class Registry:
    """Naming/lookup service with leased registrations."""

    def __init__(self) -> None:
        """Start the registry server."""
        self.entries: dict[str, tuple[dict[str, Any], float]] = {}
        self.lock = threading.Lock()
        self.server = serve(self.handle)
        self.port: int = self.server.server_address[1]

    def handle(self, msg: dict[str, Any]) -> Any:
        """Handle ``bind`` / ``lookup`` / ``list`` requests."""
        now = time.monotonic()
        with self.lock:
            self.entries = {n: e for n, e in self.entries.items() if e[1] > now}  # drop expired leases
            if msg["op"] == "bind":
                self.entries[msg["name"]] = (msg["ref"], now + msg["lease"])
                return {"ok": True}
            if msg["op"] == "lookup":
                e = self.entries.get(msg["name"])
                return {"ref": e[0]} if e else {"error": f"NotBound: {msg['name']}"}
            return {"names": sorted(self.entries)}


# ======================================================== server side: the ORB
class ObjectServer:
    """Hosts remote objects; the per-call dispatch plays the role of the skeleton."""

    def __init__(self, name: str) -> None:
        """Start the object server."""
        self.name = name
        self.objects: dict[str, Any] = {}
        self.server = serve(self.handle)
        self.port: int = self.server.server_address[1]
        self._stop = threading.Event()

    def export(self, obj: Any) -> dict[str, Any]:
        """Make ``obj`` remotely reachable and return its remote reference."""
        oid = next((k for k, v in self.objects.items() if v is obj), None) or uuid.uuid4().hex[:8]
        self.objects[oid] = obj
        return {"__ref__": True, "port": self.port, "oid": oid, "type": type(obj).__name__}

    def handle(self, msg: dict[str, Any]) -> Any:
        """Skeleton: unmarshal, invoke the method, marshal the result."""
        obj = self.objects.get(msg["oid"])
        if obj is None or msg["method"].startswith("_"):
            return {"error": "NoSuchObject/Method"}
        try:
            result = getattr(obj, msg["method"])(*msg["args"])
        except Exception as exc:  # noqa: BLE001
            return {"error": repr(exc)}
        # pass-by-value for plain data, pass-by-REFERENCE for objects
        return {"value": result if isinstance(result, PLAIN) else self.export(result)}

    def bind(self, registry_port: int, name: str, obj: Any, lease: float = 1.0) -> None:
        """Register ``obj`` under ``name`` and keep renewing the lease in background."""
        ref = self.export(obj)

        def renew() -> None:
            while not self._stop.is_set():
                call(registry_port, {"op": "bind", "name": name, "ref": ref, "lease": lease})
                self._stop.wait(lease / 3)
        start_thread(renew)

    def crash(self) -> None:
        """Die without unregistering anything."""
        self._stop.set()
        self.server.shutdown()
        self.server.server_close()
        log(self.name, "💥 crashed (no clean unbind!)")


# ======================================================== client side: stubs
# See: the same idea in Pyro5, https://pyro5.readthedocs.io/en/latest/clientcode.html
class RemoteProxy:
    """Dynamic client stub: every attribute is a remote method."""

    def __init__(self, ref: dict[str, Any]) -> None:
        """Wrap a remote reference."""
        self._ref = ref

    def __getattr__(self, method: str) -> Any:
        def stub(*args: Any) -> Any:
            reply = call(self._ref["port"], {"oid": self._ref["oid"], "method": method, "args": list(args)})
            if "error" in reply:
                raise RuntimeError(reply["error"])
            v = reply["value"]
            return RemoteProxy(v) if isinstance(v, dict) and v.get("__ref__") else v
        return stub

    def __repr__(self) -> str:
        return f"<RemoteProxy {self._ref['type']}@:{self._ref['port']}/{self._ref['oid']}>"


def lookup(registry_port: int, name: str) -> RemoteProxy:
    """Ask the naming service for ``name`` and return a proxy."""
    reply = call(registry_port, {"op": "lookup", "name": name})
    if "error" in reply:
        raise LookupError(reply["error"])
    return RemoteProxy(reply["ref"])


class Counter:
    """A stateful remote object (state lives on the server)."""

    def __init__(self) -> None:
        """Start at zero."""
        self.value = 0

    def increment(self, by: int = 1) -> int:
        """Increase and return the counter."""
        self.value += by
        return self.value


def main() -> None:
    """Run the RMI/ORB scenario."""
    banner("06 · Distributed objects: RMI/ORB, naming service & leases", "11-13")
    registry = Registry()
    log("registry", f"naming service on :{registry.port}")

    server = ObjectServer("obj-server")
    server.bind(registry.port, "WeatherService", WeatherService())
    server.bind(registry.port, "VisitCounter", Counter())
    time.sleep(0.1)

    section("Clients only know the registry: lookup by name -> remote reference -> proxy")
    log("client-1", f"registry.list() -> {call(registry.port, {'op': 'list'})['names']}")
    weather = lookup(registry.port, "WeatherService")
    log("client-1", f"lookup('WeatherService') -> {weather!r}")
    log("client-1", f"weather.get_temperature('oslo') -> {weather.get_temperature('oslo')}")

    section("Pass-by-reference: a returned object stays on the server, we get a new proxy")
    station = weather.station("bilbao")
    log("client-1", f"weather.station('bilbao') -> {station!r}")
    log("client-1", f"station.summary() executed remotely -> {station.summary()}")

    section("Remote objects keep state shared by all clients")
    c1, c2 = lookup(registry.port, "VisitCounter"), lookup(registry.port, "VisitCounter")
    log("client-1", f"counter.increment() -> {c1.increment()}")
    log("client-2", f"counter.increment(10) -> {c2.increment(10)}")
    log("client-1", f"counter.increment() -> {c1.increment()}  (sees client-2's change)")

    section("Leases: the server crashes; its registrations expire on their own")
    server.crash()
    try:
        weather.get_temperature("oslo")
    except OSError as exc:
        log("client-1", f"stale proxy fails: {type(exc).__name__}")
    time.sleep(1.2)
    try:
        lookup(registry.port, "WeatherService")
    except LookupError as exc:
        log("client-3", f"lookup after lease expiry -> {exc} (directory self-cleaned)")

    takeaway(
        "RMI/ORB = RPC + object identity: calls target a specific remote object (oid) with its own state.",
        "Plain data travels by value; objects travel as remote references (proxies).",
        "A naming/directory service decouples clients from locations (network service paradigm, Jini).",
        "Leases turn silent failures into automatic cleanup - the same idea as etcd/Consul TTLs today.",
        "Real libraries: Java RMI, CORBA, and in Python: Pyro5 (pip install Pyro5).",
    )


if __name__ == "__main__":
    main()
