"""kitchen-worker: competing consumer of the ``kitchen.orders`` work queue (Part C - TO DO).

Run two or more of them; RabbitMQ must deliver each order to exactly ONE worker.

    python kitchen_worker.py --name worker-1
    python kitchen_worker.py --name worker-2 --crash-after 3     # dies on its 3rd order WITHOUT ack
"""

from __future__ import annotations

import argparse
import json
import os
import time  # noqa: F401  (you will need it)
from typing import Any

import pika  # noqa: F401  (you will need it)

from common import EVENTS_EXCHANGE, KITCHEN_QUEUE, declare_topology, dumps, log, rabbit_connection  # noqa: F401


def publish_event(channel: Any, order: dict[str, Any], status: str, worker: str, redelivered: bool) -> None:
    """Publish a status change on the topic exchange.

    TODO C2: basic_publish to EVENTS_EXCHANGE with routing key ``order.<status in lower case>`` and a
    JSON body {"order_id", "student_id", "status", "worker", "redelivered", "ts"} (see README, message contract).
    Make it persistent (delivery_mode=2).
    """
    raise NotImplementedError("TODO C2: publish_event")


def main() -> None:
    """Consume orders forever: COOKING -> sleep(prep) -> READY -> ack."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default=f"worker-{os.getpid()}")
    ap.add_argument("--crash-after", type=int, default=0, help="simulate a crash on the N-th delivery (0 = never)")
    a = ap.parse_args()

    conn = rabbit_connection()
    ch = conn.channel()
    declare_topology(ch)
    # TODO C1: fair dispatch -> at most ONE unacknowledged message per worker (basic_qos)
    received = 0

    def on_order(chan: Any, method: Any, props: Any, body: bytes) -> None:
        nonlocal received
        received += 1
        order = json.loads(body)
        if a.crash_after and received == a.crash_after:          # GIVEN: crash simulation (keep it!)
            log(a.name, f"💥 crashing while holding order {order['order_id']} (NOT acked -> will be redelivered)")
            os._exit(1)
        # TODO C3: publish COOKING, sleep order["prep_ms"] milliseconds, publish READY,
        #          and only THEN acknowledge the message (manual ack). Pass method.redelivered to publish_event.
        raise NotImplementedError("TODO C3: cook the order")

    # TODO C4: basic_consume on KITCHEN_QUEUE with on_order and MANUAL acknowledgements (auto_ack=False)
    log(a.name, f"waiting for orders on '{KITCHEN_QUEUE}'")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


if __name__ == "__main__":
    main()
