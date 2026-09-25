"""02 · Client/server paradigm over raw sockets (slide 5).

A TCP server *listens* and *accepts*; clients *connect*, *issue a request*
and *wait for the response*. We define our own tiny application protocol:
one JSON object per line ("JSON Lines"), e.g.::

    -> {"op": "get_temperature", "args": ["bilbao"]}
    <- {"ok": true, "result": 19.2}

The server is multi-threaded, so several clients are served concurrently.
A UDP variant shows the connection-less alternative.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    nc 127.0.0.1 5000           # talk to --role server by hand; netcat: apt install netcat-openbsd | brew install netcat
    then type: {"op": "get_temperature", "args": ["bilbao"]}

Tutorials & references:
    - Socket Programming HOWTO (official tutorial)
      https://docs.python.org/3/howto/sockets.html
    - socket: low-level networking interface
      https://docs.python.org/3/library/socket.html
    - socketserver: framework for network servers
      https://docs.python.org/3/library/socketserver.html
    - Real Python: Socket Programming in Python (guide)
      https://realpython.com/python-sockets/
    - JSON Lines format
      https://jsonlines.org/

Run everything:        python examples/02_client_server_sockets/demo.py
Two terminals:         python examples/02_client_server_sockets/demo.py --role server --port 5000
                       python examples/02_client_server_sockets/demo.py --role client --port 5000
"""

from __future__ import annotations

import argparse
import json
import socket
import socketserver
import sys
import threading
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, free_port, log, section, start_thread, takeaway  # noqa: E402

SERVICE = WeatherService()
ALLOWED_OPS = {"cities", "get_temperature", "report", "summary"}


def handle_request(raw: bytes) -> dict[str, Any]:
    """Decode one request line, run it against the service, build the reply."""
    try:
        req = json.loads(raw)
        if req["op"] not in ALLOWED_OPS:
            raise ValueError(f"unknown op {req['op']!r}")
        return {"ok": True, "result": getattr(SERVICE, req["op"])(*req.get("args", []))}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


class WeatherTCPHandler(socketserver.StreamRequestHandler):
    """One instance per client connection (runs in its own thread)."""

    def handle(self) -> None:
        """Serve JSON-line requests until the client disconnects."""
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        log("server", f"accepted connection from {peer} ({threading.current_thread().name})")
        for line in self.rfile:  # blocks waiting for the next request
            reply = handle_request(line)
            self.wfile.write((json.dumps(reply) + "\n").encode())
        log("server", f"{peer} disconnected")


# See: https://docs.python.org/3/library/socketserver.html#asynchronous-mixins
class WeatherTCPServer(socketserver.ThreadingTCPServer):
    """Thread-per-connection TCP server."""

    allow_reuse_address = True
    daemon_threads = True


class WeatherClient:
    """Client-side helper hiding the socket + protocol details."""

    def __init__(self, host: str, port: int, name: str = "client") -> None:
        """Connect to the server.

        Args:
            host: Server host.
            port: Server TCP port.
            name: Label used in logs.
        """
        self.name = name
        # See: https://docs.python.org/3/library/socket.html#socket.create_connection
        self.sock = socket.create_connection((host, port))  # connect
        self.file = self.sock.makefile("rwb")

    def call(self, op: str, *args: Any) -> Any:
        """Send one request and block until its reply arrives."""
        self.file.write((json.dumps({"op": op, "args": list(args)}) + "\n").encode())
        self.file.flush()
        reply = json.loads(self.file.readline())
        log(self.name, f"{op}{args} -> {reply}")
        return reply

    def close(self) -> None:
        """Disconnect."""
        self.file.close()
        self.sock.close()


def run_clients(port: int) -> None:
    """Run a few sequential and concurrent clients against ``port``."""
    section("One client, several requests on the same TCP connection")
    c = WeatherClient("127.0.0.1", port, "client-A")
    c.call("cities")
    c.call("get_temperature", "madrid")
    c.call("report", "madrid", 27.5)
    c.call("summary", "madrid")
    c.call("delete_everything")  # protocol-level error, the connection survives
    c.close()

    section("Three concurrent clients (the threaded server serves them in parallel)")
    def worker(i: int) -> None:
        cl = WeatherClient("127.0.0.1", port, f"client-{i}")
        cl.call("report", "bilbao", 20.0 + i)
        cl.close()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    WeatherClient("127.0.0.1", port, "client-Z").call("summary", "bilbao")


def udp_demo() -> None:
    """Connection-less request/response with UDP datagrams."""
    section("Contrast: UDP (no connection, no delivery guarantee, message boundaries kept)")
    # See: https://docs.python.org/3/library/socket.html#socket.socket (UDP = SOCK_DGRAM)
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]

    def serve_once() -> None:
        data, addr = srv.recvfrom(4096)
        srv.sendto(json.dumps(handle_request(data)).encode(), addr)

    start_thread(serve_once)
    cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cli.settimeout(2)
    cli.sendto(json.dumps({"op": "get_temperature", "args": ["oslo"]}).encode(), ("127.0.0.1", port))
    log("udp-client", f"datagram reply: {cli.recvfrom(4096)[0].decode()}")
    cli.close()
    srv.close()


def main() -> None:
    """Entry point supporting ``--role demo|server|client``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["demo", "server", "client"], default="demo")
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args()

    banner("02 · Client/server with TCP sockets and a JSON-lines protocol", "5")
    port = args.port or free_port()
    if args.role in {"demo", "server"}:
        server = WeatherTCPServer(("127.0.0.1", port), WeatherTCPHandler)  # bind + listen
        log("server", f"listening on 127.0.0.1:{port}")
        if args.role == "server":
            server.serve_forever()
            return
        start_thread(lambda: server.serve_forever(poll_interval=0.05))
    run_clients(port)
    if args.role == "demo":
        server.shutdown()
        udp_demo()
        takeaway(
            "The server is passive: it only reacts to client requests (listen/accept).",
            "WE had to design the protocol (framing, encoding, errors). Higher-level paradigms do it for us.",
            "A thread per connection gives concurrency; state shared across clients needs locking.",
            "TCP = reliable byte stream; UDP = unreliable datagrams with no connection.",
        )


if __name__ == "__main__":
    main()
