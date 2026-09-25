"""04 · Message-system paradigm / Message-Oriented Middleware (slides 7-9).

A **broker** sits between independent processes, so they exchange messages
*asynchronously* and *decoupled* in space (they don't know each other) and in
time (they don't need to be up at the same moment).

1. **Point-to-point** (queue): each message is consumed by exactly ONE consumer.
   Competing consumers give load balancing; acknowledgements give reliability
   (a message whose consumer crashes before ACK is redelivered).
2. **Publish/subscribe** (topic): each message is copied to EVERY current
   subscriber. Subscribers that join later miss earlier messages.

(Example 14 shows the same ideas with AMQP exchanges; 17 with a persistent log.)

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Real brokers: see example 14 (RabbitMQ) and 17 (Kafka)

Tutorials & references:
    - Enterprise Integration Patterns: messaging
      https://www.enterpriseintegrationpatterns.com/patterns/messaging/
    - EIP: Point-to-Point Channel
      https://www.enterpriseintegrationpatterns.com/patterns/messaging/PointToPointChannel.html
    - EIP: Publish-Subscribe Channel
      https://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html
    - EIP: Competing Consumers
      https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html
    - queue: synchronized queue class
      https://docs.python.org/3/library/queue.html

Run:  python examples/04_message_system/demo.py
"""

from __future__ import annotations

import itertools
import queue
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402


@dataclass
class Delivery:
    """A message handed to a consumer, pending acknowledgement."""

    tag: int
    queue: str
    body: Any


class Broker:
    """A minimal message broker with durable-ish queues and topics."""

    def __init__(self) -> None:
        """Create an empty broker."""
        self._queues: dict[str, queue.Queue[Any]] = {}
        self._topics: dict[str, list[queue.Queue[Any]]] = {}
        self._unacked: dict[int, Delivery] = {}
        self._tags = itertools.count(1)
        self._lock = threading.Lock()

    # -------------------------------------------------- point-to-point
    def send(self, qname: str, body: Any) -> None:
        """Enqueue a message (the sender does not wait for any consumer)."""
        self._queues.setdefault(qname, queue.Queue()).put(body)

    def receive(self, qname: str, timeout: float = 0.5) -> Delivery | None:
        """Take the next message; it stays 'unacked' until :meth:`ack`."""
        try:
            body = self._queues.setdefault(qname, queue.Queue()).get(timeout=timeout)
        except queue.Empty:
            return None
        d = Delivery(next(self._tags), qname, body)
        with self._lock:
            self._unacked[d.tag] = d
        return d

    def ack(self, d: Delivery) -> None:
        """Confirm processing: the broker can forget the message."""
        with self._lock:
            self._unacked.pop(d.tag, None)

    def connection_lost(self, deliveries: list[Delivery]) -> None:
        """A consumer died: requeue what it had not acknowledged."""
        with self._lock:
            for d in deliveries:
                if self._unacked.pop(d.tag, None):
                    self._queues[d.queue].put(d.body)
                    log("broker", f"requeued unacked message {d.body!r}")

    # -------------------------------------------------- publish/subscribe
    def subscribe(self, topic: str) -> queue.Queue[Any]:
        """Register a subscriber; returns its private inbox."""
        inbox: queue.Queue[Any] = queue.Queue()
        self._topics.setdefault(topic, []).append(inbox)
        return inbox

    def publish(self, topic: str, body: Any) -> int:
        """Copy the message to every current subscriber. Returns #copies."""
        subs = self._topics.get(topic, [])
        for inbox in subs:
            inbox.put(body)
        return len(subs)


def worker(broker: Broker, name: str, handled: Counter[str], crash_at: int | None = None) -> None:
    """Competing consumer that processes jobs from the ``jobs`` queue.

    Args:
        broker: The broker to consume from.
        name: Worker label.
        handled: Shared counter of processed jobs per worker.
        crash_at: If set, the worker "crashes" on its N-th delivery (before ACK).
    """
    received = 0
    while (d := broker.receive("jobs")) is not None:
        received += 1
        if received == crash_at:
            log(name, f"received {d.body!r} ... 💥 CRASHED before ACK")
            broker.connection_lost([d])
            return
        time.sleep(0.05)  # simulated work
        broker.ack(d)
        handled[name] += 1
        log(name, f"processed + acked {d.body!r}")


def subscriber(name: str, inbox: queue.Queue[Any], got: list[str]) -> None:
    """Pub/sub consumer: prints whatever arrives on its inbox."""
    while (msg := inbox.get()) is not None:
        got.append(msg)
        log(name, f"got {msg!r}")


def main() -> None:
    """Run the point-to-point and publish/subscribe scenarios."""
    banner("04 · Message system: point-to-point queues vs publish/subscribe", "7-9")
    broker = Broker()

    section("Point-to-point: the producer sends while NO consumer is running (time decoupling)")
    for i in range(8):
        broker.send("jobs", f"compute-daily-avg-{i}")
    log("producer", "sent 8 jobs and exits - it never knew who would process them")

    section("Three competing consumers start later; one crashes before acknowledging")
    handled: Counter[str] = Counter()
    ws = [threading.Thread(target=worker, args=(broker, f"worker-{i}", handled,
                                                   2 if i == 0 else None))
          for i in range(3)]
    for w in ws:
        w.start()
    for w in ws:
        w.join()
    log("stats", f"jobs per worker: {dict(handled)} -> total {sum(handled.values())} (each acked exactly once)")

    section("Publish/subscribe: every subscriber receives its own copy")
    got: dict[str, list[str]] = {n: [] for n in ("dashboard", "sms-alerts", "audit-log")}
    inboxes = {n: broker.subscribe("weather.alerts") for n in ("dashboard", "sms-alerts")}
    threads = {n: threading.Thread(target=subscriber, args=(n, ib, got[n])) for n, ib in inboxes.items()}
    for t in threads.values():
        t.start()
    n = broker.publish("weather.alerts", "HEAT: madrid 41°C")
    log("publisher", f"published to {n} subscribers")
    time.sleep(0.1)
    inboxes["audit-log"] = broker.subscribe("weather.alerts")  # late joiner
    threads["audit-log"] = threading.Thread(target=subscriber, args=("audit-log", inboxes["audit-log"], got["audit-log"]))
    threads["audit-log"].start()
    n = broker.publish("weather.alerts", "STORM: bilbao wind 90km/h")
    log("publisher", f"published to {n} subscribers")
    time.sleep(0.1)
    for ib in inboxes.values():
        ib.put(None)
    for t in threads.values():
        t.join()
    log("stats", {k: len(v) for k, v in got.items()} | {"note": "audit-log joined late and missed the HEAT alert"})

    takeaway(
        "Queues: one message -> one consumer. Add consumers to scale out (competing consumers).",
        "ACKs make delivery at-least-once: crashed work is redelivered (so handlers should be idempotent).",
        "Topics: one message -> N subscribers; publishers don't know who listens.",
        "Classic pub/sub does not keep history: late subscribers miss messages (see 17 for log-based streaming).",
    )


if __name__ == "__main__":
    main()
