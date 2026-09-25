"""Shared configuration and helpers for DeustoEats (GIVEN - you do not need to change this file).

Everything is configurable with environment variables so the same code runs on a laptop,
in Docker or in the cloud.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent

# ----------------------------------------------------------------------------- configuration
MENU_ADDR: str = os.environ.get("MENU_ADDR", "127.0.0.1:50061")      # gRPC menu-service
API_HOST: str = os.environ.get("API_HOST", "127.0.0.1")
API_PORT: int = int(os.environ.get("API_PORT", "8081"))              # REST orders-api
RABBIT_HOST: str = os.environ.get("RABBIT_HOST", "localhost")        # RabbitMQ broker
NOTIFICATIONS_FILE: Path = Path(os.environ.get("NOTIFICATIONS_FILE", HERE / "notifications.log"))

# ----------------------------------------------------------------------------- messaging contract
KITCHEN_QUEUE = "kitchen.orders"        # work queue (point-to-point, durable)
EVENTS_EXCHANGE = "order.events"        # topic exchange (publish/subscribe)
NOTIFIER_QUEUE = "notifier.sms"         # the notifier's own queue, bound to "order.ready"
STATUS_ORDER = ["PENDING", "COOKING", "READY"]

# ----------------------------------------------------------------------------- menu (menu-service data)
MENU: dict[str, dict[str, Any]] = {
    "pintxo-tortilla": {"name": "Pintxo de tortilla", "price_cents": 250, "stock": 50, "prep_ms": 300},
    "bocadillo-jamon": {"name": "Bocadillo de jamón", "price_cents": 450, "stock": 20, "prep_ms": 500},
    "cafe": {"name": "Café con leche", "price_cents": 120, "stock": 100, "prep_ms": 100},
    "ensalada": {"name": "Ensalada mixta", "price_cents": 550, "stock": 5, "prep_ms": 400},
    "gilda": {"name": "Gilda", "price_cents": 200, "stock": 0, "prep_ms": 100},   # sold out
}
MAX_PREP_MS = 2500


def log(who: str, msg: str) -> None:
    """Time-stamped log line on stdout (flushed, so it interleaves nicely between processes)."""
    print(f"{time.strftime('%H:%M:%S')} [{who:>12}] {msg}", flush=True)


def compile_proto() -> None:
    """Generate menu_pb2.py / menu_pb2_grpc.py into ./generated (idempotent) and make them importable."""
    out = HERE / "generated"
    if not (out / "menu_pb2_grpc.py").exists():
        from grpc_tools import protoc

        out.mkdir(exist_ok=True)
        rc = protoc.main(["protoc", f"-I{HERE}", f"--python_out={out}", f"--grpc_python_out={out}",
                          str(HERE / "menu.proto")])
        if rc != 0:
            raise RuntimeError("protoc failed")
    if str(out) not in sys.path:
        sys.path.insert(0, str(out))


def rabbit_connection(retries: int = 30) -> Any:
    """Open a pika BlockingConnection, retrying while the broker starts."""
    import pika

    for attempt in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST, heartbeat=30))
        except pika.exceptions.AMQPConnectionError:
            if attempt == retries - 1:
                raise SystemExit(f"Cannot connect to RabbitMQ at {RABBIT_HOST}:5672. Start it with:\n"
                                 "  docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management")
            time.sleep(1)


def declare_topology(channel: Any) -> None:
    """Declare the queue and exchange every component relies on (declarations are idempotent)."""
    channel.queue_declare(queue=KITCHEN_QUEUE, durable=True)
    channel.exchange_declare(exchange=EVENTS_EXCHANGE, exchange_type="topic", durable=True)


def dumps(obj: Any) -> bytes:
    """JSON-encode a message body."""
    return json.dumps(obj).encode()
