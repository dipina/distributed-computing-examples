"""13 · Real-time web: polling vs long polling vs Server-Sent Events vs WebSocket (slides 59-63).

HTTP is request/response: the server cannot speak first. How do we get
server-side events (new temperature readings) to a client quickly?

1. **Short polling**  - ask every N ms. Simple, but wasteful and adds latency.
2. **Long polling**   - the server *holds* the request until there is news.
3. **SSE**            - one long-lived HTTP response streaming ``text/event-stream``
                        (server -> client only; auto-reconnect in browsers).
4. **WebSocket**      - an upgraded, full-duplex TCP channel (both directions).

A background "sensor" produces a reading every 250 ms. Each client listens for
~2 s; we compare HTTP requests made, events received and delivery latency.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install fastapi uvicorn httpx websockets
    curl -N http://127.0.0.1:<port>/sse                 # watch an SSE stream by hand
    python -m websockets ws://127.0.0.1:<port>/ws       # interactive WebSocket client

Tutorials & references:
    - MDN: Using server-sent events
      https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
    - MDN: The WebSocket API
      https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
    - HTML standard: server-sent events
      https://html.spec.whatwg.org/multipage/server-sent-events.html
    - RFC 6455: The WebSocket Protocol
      https://www.rfc-editor.org/rfc/rfc6455
    - FastAPI: WebSockets
      https://fastapi.tiangolo.com/advanced/websockets/
    - websockets library documentation
      https://websockets.readthedocs.io/

Run:       python examples/13_realtime_web/demo.py
Requires:  pip install fastapi uvicorn httpx websockets
"""

from __future__ import annotations

import asyncio
import json
import random
import statistics
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import UvicornThread, banner, log, require, section, takeaway  # noqa: E402

require("fastapi", "fastapi uvicorn")
httpx = require("httpx")
websockets = require("websockets")
from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402
from websockets.sync.client import connect as ws_connect  # noqa: E402

PERIOD = 0.25
LISTEN = 2.0


class EventBus:
    """Holds the latest events and wakes up waiters (asyncio)."""

    def __init__(self) -> None:
        """Start empty."""
        self.events: list[dict[str, Any]] = []
        self.cond = asyncio.Condition()

    async def publish(self, ev: dict[str, Any]) -> None:
        """Store an event and notify everybody waiting."""
        async with self.cond:
            self.events.append(ev)
            self.cond.notify_all()

    async def wait_after(self, seq: int, timeout: float) -> list[dict[str, Any]]:
        """Return events with ``seq`` greater than the given one, waiting up to ``timeout``."""
        async with self.cond:
            try:
                await asyncio.wait_for(self.cond.wait_for(lambda: self.events and self.events[-1]["seq"] > seq), timeout)
            except asyncio.TimeoutError:
                return []
            return [e for e in self.events if e["seq"] > seq]


def create_app() -> FastAPI:
    """Build the app with the four real-time endpoints."""
    bus = EventBus()

    async def sensor() -> None:
        seq = 0
        while True:
            await asyncio.sleep(PERIOD)
            seq += 1
            await bus.publish({"seq": seq, "city": "bilbao", "celsius": round(random.uniform(17, 22), 1), "ts": time.time()})

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(sensor())
        yield
        task.cancel()

    app = FastAPI(lifespan=lifespan)

    @app.get("/latest")
    async def latest() -> dict[str, Any]:
        """Short polling endpoint: answer immediately with the latest event."""
        return bus.events[-1] if bus.events else {}

    @app.get("/long-poll")
    async def long_poll(after: int = 0) -> list[dict[str, Any]]:
        """Long polling endpoint: hold the request until something newer than ``after`` exists."""
        return await bus.wait_after(after, timeout=10)

    @app.get("/sse")
    async def sse() -> StreamingResponse:
        """Server-Sent Events stream."""
        async def gen() -> AsyncIterator[str]:
            last = bus.events[-1]["seq"] if bus.events else 0
            while True:
                for ev in await bus.wait_after(last, timeout=10):
                    last = ev["seq"]
                    yield f"id: {ev['seq']}\nevent: reading\ndata: {json.dumps(ev)}\n\n"
        # See: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
        return StreamingResponse(gen(), media_type="text/event-stream")

    # See: https://fastapi.tiangolo.com/advanced/websockets/
    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        """Full-duplex channel: the client can send commands while receiving events."""
        await socket.accept()
        threshold = -100.0
        last = bus.events[-1]["seq"] if bus.events else 0

        async def receive_commands() -> None:
            nonlocal threshold
            while True:
                cmd = json.loads(await socket.receive_text())
                threshold = float(cmd["threshold"])
                await socket.send_text(json.dumps({"ack": f"threshold set to {threshold}"}))

        reader = asyncio.create_task(receive_commands())
        try:
            while True:
                for ev in await bus.wait_after(last, timeout=10):
                    last = ev["seq"]
                    if ev["celsius"] >= threshold:
                        await socket.send_text(json.dumps(ev))
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            reader.cancel()

    return app


def report(name: str, requests: int, latencies: list[float], extra: str = "") -> dict[str, Any]:
    """Log and return the statistics of one technique."""
    avg = statistics.fmean(latencies) * 1000 if latencies else float("nan")
    log(name, f"HTTP requests={requests:3d}  events={len(latencies):2d}  avg latency={avg:6.1f} ms {extra}")
    return {"technique": name, "requests": requests, "events": len(latencies), "latency_ms": round(avg, 1)}


def short_polling(base: str) -> dict[str, Any]:
    """Ask every 100 ms whether something changed."""
    seen, lat, reqs, t_end = 0, [], 0, time.time() + LISTEN
    with httpx.Client(base_url=base) as c:
        while time.time() < t_end:
            ev = c.get("/latest").json()
            reqs += 1
            if ev and ev["seq"] > seen:
                seen = ev["seq"]
                lat.append(time.time() - ev["ts"])
            time.sleep(0.1)
    return report("short-poll", reqs, lat, f"({reqs - len(lat)} wasted requests)")


def long_polling(base: str) -> dict[str, Any]:
    """Re-issue a held request as soon as the previous one returns."""
    seen, lat, reqs, t_end = 0, [], 0, time.time() + LISTEN
    with httpx.Client(base_url=base, timeout=15) as c:
        seen = (c.get("/latest").json() or {"seq": 0})["seq"]
        while time.time() < t_end:
            evs = c.get("/long-poll", params={"after": seen}).json()
            reqs += 1
            for ev in evs:
                seen = ev["seq"]
                lat.append(time.time() - ev["ts"])
    return report("long-poll", reqs, lat)


def server_sent_events(base: str) -> dict[str, Any]:
    """One request; parse the text/event-stream as it arrives."""
    lat, t_end = [], time.time() + LISTEN
    with httpx.Client(base_url=base, timeout=15) as c, c.stream("GET", "/sse") as r:
        log("sse", f"Content-Type: {r.headers['content-type']}")
        for line in r.iter_lines():
            if line.startswith("data: "):
                lat.append(time.time() - json.loads(line[6:])["ts"])
            if time.time() > t_end:
                break
    return report("sse", 1, lat)


def websocket(base: str) -> dict[str, Any]:
    """Receive events AND send a command on the same connection."""
    lat, t_end, sent = [], time.time() + LISTEN, False
    # See: https://websockets.readthedocs.io/en/stable/reference/sync/client.html
    with ws_connect(base.replace("http", "ws") + "/ws") as ws:
        while time.time() < t_end:
            msg = json.loads(ws.recv(timeout=5))
            if "ack" in msg:
                log("websocket", f"server ack on the same socket: {msg['ack']}")
                continue
            lat.append(time.time() - msg["ts"])
            if not sent and time.time() > t_end - LISTEN / 2:
                ws.send(json.dumps({"threshold": 20.0}))  # client -> server while streaming
                log("websocket", "client -> server: only send readings >= 20°C from now on")
                sent = True
    return report("websocket", 1, lat, "(1 HTTP upgrade, then full duplex)")


def main() -> None:
    """Run the four techniques one after another against the same server."""
    banner("13 · Real-time web: polling, long polling, SSE, WebSocket", "59-63")
    with UvicornThread(create_app()) as srv:
        time.sleep(0.3)
        rows = []
        for title, fn in [("Short polling every 100 ms", short_polling), ("Long polling", long_polling),
                          ("Server-Sent Events (server -> client stream)", server_sent_events),
                          ("WebSocket (bidirectional)", websocket)]:
            section(title)
            rows.append(fn(srv.url))
    section("Comparison")
    print(f"  {'technique':<12}{'requests':>10}{'events':>8}{'latency ms':>12}")
    for r in rows:
        print(f"  {r['technique']:<12}{r['requests']:>10}{r['events']:>8}{r['latency_ms']:>12}")
    takeaway(
        "Short polling wastes requests and its latency depends on the polling interval.",
        "Long polling gives near-immediate delivery with plain HTTP, at one request per event.",
        "SSE: one HTTP response streamed forever; perfect for server->client feeds (and LLM token streaming!).",
        "WebSocket: full duplex after an HTTP upgrade - chats, games, collaborative editors.",
    )


if __name__ == "__main__":
    main()
