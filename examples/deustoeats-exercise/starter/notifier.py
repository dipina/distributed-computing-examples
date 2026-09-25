"""notifier: publish/subscribe consumer that "sends an SMS" when an order is READY (Part D - TO DO).

    python notifier.py
"""

from __future__ import annotations

import json  # noqa: F401  (you will need it)
import time  # noqa: F401
from typing import Any  # noqa: F401

from common import (EVENTS_EXCHANGE, NOTIFICATIONS_FILE, NOTIFIER_QUEUE, declare_topology, log,  # noqa: F401
                    rabbit_connection)


def main() -> None:
    """Consume READY events and append one notification line per order to NOTIFICATIONS_FILE.

    TODO D1: declare your own durable queue NOTIFIER_QUEUE and BIND it to EVENTS_EXCHANGE with the routing
             key that selects ONLY the "ready" events (the notifier must never see "order.cooking").
    TODO D2: for each event append a line to NOTIFICATIONS_FILE that contains at least the order id, e.g.
             "SMS to s042: your order 1a2b3c4d is READY (cooked by worker-1)", then ack the message.
    """
    conn = rabbit_connection()
    ch = conn.channel()
    declare_topology(ch)
    raise NotImplementedError("TODO D1/D2: notifier")


if __name__ == "__main__":
    main()
