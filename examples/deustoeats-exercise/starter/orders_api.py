"""orders-api: public REST API of DeustoEats (Parts B, E and F - TO DO).

It is the only component clients talk to, and it glues the other paradigms together:

* REST (client/server over HTTP) ........ the endpoints below                                  (Part B)
* RPC (gRPC) ............................ Quote() on the menu-service                          (Part B)
* Message queue (point-to-point) ........ publish each accepted order to ``kitchen.orders``    (Part B)
* Publish/subscribe ..................... subscribe to ``order.*`` on ``order.events``         (Part E)
* Server push (SSE) ..................... GET /orders/{id}/events                              (Part E)
* Idempotent POST ....................... Idempotency-Key header                               (Part F)

    python orders_api.py          # http://127.0.0.1:8081/docs
"""

from __future__ import annotations

import asyncio  # noqa: F401  (you will need it for the SSE stream)
import json
import threading
import time  # noqa: F401  (you will need it)
import uuid  # noqa: F401
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import grpc  # noqa: F401  (you will need it)
import pika  # noqa: F401  (you will need it)
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse  # noqa: F401
from pydantic import BaseModel, Field  # noqa: F401  (you will need it)

from common import (API_HOST, API_PORT, EVENTS_EXCHANGE, KITCHEN_QUEUE, MENU_ADDR, STATUS_ORDER, compile_proto,  # noqa: F401
                    declare_topology, dumps, log, rabbit_connection)

compile_proto()
import menu_pb2 as pb  # noqa: E402
import menu_pb2_grpc as pb_grpc  # noqa: E402,F401

GRPC_DEADLINE_S = 1.0


# =============================================================================== REST resources
class Line(BaseModel):
    """One order line.  TODO B1: sku must be non-empty; qty must be > 0 and <= 20 (use Field)."""

    sku: str
    qty: int


class OrderIn(BaseModel):
    """Body of POST /orders.  TODO B1: student_id 1..20 chars; at least one line."""

    student_id: str
    lines: list[Line]


# =============================================================================== state (thread-safe, GIVEN except E2)
class OrderStore:
    """In-memory order repository shared by the HTTP threads and the event-listener thread."""

    def __init__(self) -> None:
        """Empty store."""
        self.lock = threading.Lock()
        self.orders: dict[str, dict[str, Any]] = {}
        self.idempotency: dict[str, str] = {}      # Idempotency-Key -> order id

    def get(self, order_id: str) -> dict[str, Any] | None:
        """Return a copy of an order (or None)."""
        with self.lock:
            o = self.orders.get(order_id)
            return json.loads(json.dumps(o)) if o else None

    def by_key(self, key: str | None) -> dict[str, Any] | None:
        """Order previously created with this Idempotency-Key."""
        with self.lock:
            oid = self.idempotency.get(key) if key else None
        return self.get(oid) if oid else None

    def create(self, order: dict[str, Any], key: str | None) -> tuple[dict[str, Any], bool]:
        """Insert atomically. Returns (order, created); if the key was taken meanwhile, returns the winner."""
        with self.lock:
            if key and key in self.idempotency:
                return self.orders[self.idempotency[key]], False
            self.orders[order["id"]] = order
            if key:
                self.idempotency[key] = order["id"]
            return order, True

    def delete(self, order_id: str) -> None:
        """Remove an order (compensation when it cannot be sent to the kitchen)."""
        with self.lock:
            self.orders.pop(order_id, None)
            self.idempotency = {k: v for k, v in self.idempotency.items() if v != order_id}

    def list(self, status_: str | None, student_id: str | None) -> list[dict[str, Any]]:
        """Filter the collection."""
        with self.lock:
            return [json.loads(json.dumps(o)) for o in self.orders.values()
                    if (status_ is None or o["status"] == status_) and (student_id is None or o["student_id"] == student_id)]

    def apply_event(self, ev: dict[str, Any]) -> bool:
        """Apply a status event received from the kitchen. Returns True if the order changed.

        TODO E2: make it IDEMPOTENT - the status may only move FORWARD (PENDING -> COOKING -> READY, see
        STATUS_ORDER); duplicated or late events are ignored. Append {"status", "at", "by", "redelivered"}
        to order["history"] and set order["cooked_by"] when the status becomes READY.
        Remember to hold self.lock.
        """
        raise NotImplementedError("TODO E2: apply_event")


STORE = OrderStore()


# =============================================================================== messaging
class KitchenPublisher:
    """Publishes orders to the work queue. pika connections are not thread-safe -> one lock (GIVEN)."""

    def __init__(self) -> None:
        """Lazy connection."""
        self.lock = threading.Lock()
        self.conn: Any = None
        self.ch: Any = None

    def _connect(self) -> None:
        self.conn = rabbit_connection(retries=3)
        self.ch = self.conn.channel()
        declare_topology(self.ch)
        self.ch.confirm_delivery()              # publisher confirms: the broker acknowledges our publish

    def publish(self, message: dict[str, Any]) -> None:
        """Send a PERSISTENT message to the kitchen work queue.

        TODO B4: with self.lock held, (re)connect if needed and basic_publish ``dumps(message)`` to the
        DEFAULT exchange ("") with routing key KITCHEN_QUEUE and pika.BasicProperties(delivery_mode=2).
        """
        raise NotImplementedError("TODO B4: publish")


PUBLISHER = KitchenPublisher()


def listen_events(stop: threading.Event) -> None:
    """Background thread: subscribe to every order event and update the store.

    TODO E1: declare a private queue (queue_declare(queue="", exclusive=True)), bind it to EVENTS_EXCHANGE
    with a routing key that matches ALL order events, consume with manual ack and call STORE.apply_event().
    Keep the loop below so the thread stops with the server.
    """
    conn = rabbit_connection()
    ch = conn.channel()
    declare_topology(ch)
    # ... TODO E1 ...
    while not stop.is_set():
        conn.process_data_events(time_limit=0.5)
    conn.close()


# =============================================================================== RPC client
MENU_STUB = None


def quote(lines: list[Line]) -> pb.QuoteReply:
    """Call Menu.Quote over gRPC and translate gRPC errors into HTTP errors.

    TODO B2: create (once) a pb_grpc.MenuStub over grpc.insecure_channel(MENU_ADDR); call Quote with
    timeout=GRPC_DEADLINE_S; map grpc.RpcError codes: NOT_FOUND->404, FAILED_PRECONDITION->409,
    INVALID_ARGUMENT->422, anything else (UNAVAILABLE, DEADLINE_EXCEEDED...)->503 (raise HTTPException).
    """
    raise HTTPException(501, "TODO B2: quote")


# =============================================================================== REST API
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start/stop the event-listener thread with the web server (GIVEN)."""
    stop = threading.Event()
    threading.Thread(target=listen_events, args=(stop,), daemon=True).start()
    yield
    stop.set()


app = FastAPI(title="DeustoEats orders-api", version="1.0", lifespan=lifespan)


def represent(o: dict[str, Any]) -> dict[str, Any]:
    """Resource representation with hypermedia links (GIVEN)."""
    return {**o, "links": {"self": f"/orders/{o['id']}", "events": f"/orders/{o['id']}/events"}}


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe (GIVEN)."""
    return {"status": "up"}


@app.post("/orders", status_code=status.HTTP_201_CREATED)
def create_order(body: OrderIn, response: Response,
                 idempotency_key: str | None = Header(None, alias="Idempotency-Key")) -> dict[str, Any]:
    """Create an order.

    TODO B3: quote() the lines, build the order dict (see README: id, student_id, status "PENDING", lines,
             total_cents, prep_ms, cooked_by None, created_at, history [PENDING]), STORE.create() it,
             PUBLISHER.publish() {"order_id", "student_id", "lines", "prep_ms"} to the kitchen, set the
             Location header and return represent(STORE.get(order_id)) with 201 (a SNAPSHOT copy: the
             event-listener thread may modify the stored dict at any time). If publishing fails: delete the order
             and answer 503.
    TODO F1: if the Idempotency-Key was already used, return the SAME order with 200 and do NOT publish again.
    """
    raise HTTPException(501, "TODO B3/F1: create_order")


@app.get("/orders")
def list_orders(status_: str | None = Query(None, alias="status", pattern="^(PENDING|COOKING|READY)$"),
                student_id: str | None = None) -> list[dict[str, Any]]:
    """Collection with filters.  TODO B5: return represent() of STORE.list(...)."""
    raise HTTPException(501, "TODO B5: list_orders")


@app.get("/orders/{order_id}")
def get_order(order_id: str) -> dict[str, Any]:
    """One order.  TODO B5: 404 if unknown."""
    raise HTTPException(501, "TODO B5: get_order")


@app.get("/orders/{order_id}/events")
async def order_events(order_id: str) -> StreamingResponse:
    """Server-Sent Events.

    TODO E3: 404 if unknown; otherwise return StreamingResponse(generator, media_type="text/event-stream").
    The async generator emits ``event: status\\ndata: {"id": ..., "status": ..., "cooked_by": ...}\\n\\n``
    every time the status changes (check every 0.1 s with await asyncio.sleep) and RETURNS after READY.
    """
    raise HTTPException(501, "TODO E3: SSE")


def main() -> None:
    """Serve with uvicorn (GIVEN)."""
    log("orders-api", f"REST API on http://{API_HOST}:{API_PORT} (docs at /docs)")
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="warning")


if __name__ == "__main__":
    main()
