"""08 · Mobile-agent paradigm (slide 13).

A mobile agent is a program **plus its state** that travels from host to host.
At each host it runs *locally* against that host's resources and then migrates
to the next host in its itinerary, finally returning home with its results.

Here each host is a separate OS process (with its own TCP port) holding the
private readings of one weather station. The agent's *source code* and *state*
travel as JSON; hosts load and execute it (``exec``) in a restricted namespace.

Why? Move the computation to the data: only the (small) agent travels, never
the (large) raw data. Same idea as "code shipping" in Spark/Hadoop, or
edge-computing functions pushed to devices.

SECURITY NOTE: executing received code is dangerous. The restricted builtins
here are NOT a real sandbox - real systems use signed code, WebAssembly
sandboxes, containers, etc. Only run this demo on localhost.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install wasmtime        # optional: a REAL sandbox for travelling code (WebAssembly)

Tutorials & references:
    - exec() and its security caveats
      https://docs.python.org/3/library/functions.html#exec
    - inspect.getsource (shipping source code)
      https://docs.python.org/3/library/inspect.html#inspect.getsource
    - Mobile agent (overview)
      https://en.wikipedia.org/wiki/Mobile_agent
    - WebAssembly
      https://webassembly.org/
    - wasmtime-py: run Wasm from Python
      https://github.com/bytecodealliance/wasmtime-py

Run:  python examples/08_mobile_agents/demo.py
"""

from __future__ import annotations

import builtins
import inspect
import json
import multiprocessing as mp
import socket
import socketserver
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import INITIAL_READINGS  # noqa: E402
from common.utils import banner, log, section, takeaway  # noqa: E402

SAFE_BUILTINS = {"max": max, "min": min, "len": len, "round": round, "sum": sum, "sorted": sorted,
                 "dict": dict, "list": list, "__build_class__": builtins.__build_class__}


class HeatwaveScout:
    """The mobile agent. MUST be self-contained: its source travels over the network."""

    def __init__(self, state: dict) -> None:
        self.state = state

    def visit(self, host: dict) -> None:
        """Run at each host against local data only."""
        readings = host["readings"]
        self.state["visited"].append(host["name"])
        self.state["raw_values_seen"] += len(readings)
        hottest = max(readings)
        if hottest >= self.state["threshold"]:
            self.state["alerts"].append({"city": host["city"], "max": hottest})
        best = self.state["hottest"]
        if best is None or hottest > best["max"]:
            self.state["hottest"] = {"city": host["city"], "max": hottest}


def send(port: int, payload: dict[str, Any]) -> None:
    """Ship a serialised agent to the host listening on ``port``."""
    with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
        s.sendall((json.dumps(payload) + "\n").encode())


def host_main(name: str, city: str, readings: list[float], ports: dict[str, int], ready: Any) -> None:
    """A host process: receive agents, run them locally, forward them."""

    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            msg = json.loads(self.rfile.readline())
            ns: dict[str, Any] = {"__builtins__": SAFE_BUILTINS, "__name__": "agent"}
            exec(msg["code"], ns)  # noqa: S102 - load the travelling code
            agent = ns[msg["cls"]](msg["state"])
            agent.visit({"name": name, "city": city, "readings": readings})
            log(name, f"agent ran locally on {len(readings)} private readings; "
                      f"state size now {len(json.dumps(agent.state))} bytes")
            nxt = msg["itinerary"].pop(0)
            log(name, f"migrating agent -> {nxt}")
            send(ports[nxt], {**msg, "state": agent.state})

    srv = socketserver.TCPServer(("127.0.0.1", ports[name]), Handler)
    ready.set()
    srv.serve_forever()


def free_ports(n: int) -> list[int]:
    """Reserve ``n`` distinct free ports."""
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


def main() -> None:
    """Launch hosts, dispatch the agent, wait for it to come home."""
    banner("08 · Mobile agents: move the code to the data", "13")
    hosts = {f"host-{c}": (c, r * 50) for c, r in INITIAL_READINGS.items()}  # pretend lots of data
    names = [*hosts, "home"]
    ports = dict(zip(names, free_ports(len(names))))
    procs = []
    for name, (city, readings) in hosts.items():
        ready = mp.Event()
        p = mp.Process(target=host_main, args=(name, city, readings, ports, ready), daemon=True)
        p.start()
        ready.wait(5)
        procs.append(p)
    log("home", f"started {len(hosts)} host processes: {list(hosts)}")

    home = socketserver.TCPServer(("127.0.0.1", ports["home"]), socketserver.StreamRequestHandler)

    section("Dispatch: serialise code + state + itinerary and send it to the first host")
    itinerary = [*hosts, "home"]
    payload = {"cls": "HeatwaveScout", "code": inspect.getsource(HeatwaveScout), "itinerary": itinerary[1:],
               "state": {"threshold": 25.0, "visited": [], "alerts": [], "hottest": None, "raw_values_seen": 0}}
    log("home", f"agent payload is {len(json.dumps(payload))} bytes; itinerary: {' -> '.join(itinerary)}")
    send(ports[itinerary[0]], payload)

    home.socket.settimeout(15)
    conn, _ = home.socket.accept()
    back = json.loads(conn.makefile().readline())
    conn.close()
    home.server_close()

    section("The agent is back home with its results")
    st = back["state"]
    log("home", f"visited: {st['visited']}")
    log("home", f"hottest: {st['hottest']}, alerts (>= {st['threshold']}°C): {st['alerts']}")
    raw_bytes = len(json.dumps([r for _, r in hosts.values()]))
    log("home", f"raw readings examined remotely: {st['raw_values_seen']} values (~{raw_bytes} bytes) "
                f"that never crossed the network")
    for p in procs:
        p.terminate()

    takeaway(
        "The agent carries code AND state; each hop runs locally, close to the data.",
        "Only a small payload travels, and hosts can be disconnected between hops.",
        "Big risk: executing foreign code. Today: sandboxes (WebAssembly, containers), signed code.",
        "Descendants: Spark/MapReduce code shipping, edge functions, and LLM agents that 'travel' via tool calls (see 23).",
    )


if __name__ == "__main__":
    main()
