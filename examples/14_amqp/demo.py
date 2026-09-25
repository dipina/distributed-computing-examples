"""14 · AMQP: exchanges, bindings and queues (slides 64-71).

In AMQP 0-9-1 (RabbitMQ) producers never publish to a queue directly. They
publish to an **exchange** with a **routing key** (and headers); the exchange
routes copies to the **queues** that are **bound** to it following its type:

=========  ================================================================
default    direct exchange with no name; every queue is bound by its own name
direct     unicast: binding key == routing key
fanout     broadcast to every bound queue (routing key ignored)
topic      pattern match on dotted keys: ``*`` = one word, ``#`` = zero or more
headers    match on message headers (``x-match: all | any``)
=========  ================================================================

Part 1 runs an in-process **mini-broker** that implements these rules so the
demo works anywhere. Part 2 repeats a topic-exchange scenario against a
**real RabbitMQ** (via ``pika``) if one is reachable on localhost:5672:

    docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
    pip install pika

(``rabbitmq_send.py`` / ``rabbitmq_receive.py`` are the classic two-terminal "hello world".)

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install pika
    docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
    management UI: http://localhost:15672  (user guest / password guest)
    Other installers (Windows, Debian, macOS): https://www.rabbitmq.com/docs/download

Tutorials & references:
    - RabbitMQ tutorials (Python, pika)
      https://www.rabbitmq.com/tutorials
    - AMQP 0-9-1 model explained
      https://www.rabbitmq.com/tutorials/amqp-concepts
    - Tutorial 5: topic exchanges (Python)
      https://www.rabbitmq.com/tutorials/tutorial-five-python
    - AMQP 0-9-1 complete reference
      https://www.rabbitmq.com/amqp-0-9-1-reference
    - Pika documentation
      https://pika.readthedocs.io/
    - A quick guide to understanding RabbitMQ & AMQP (slide 71)
      https://medium.com/swlh/a-quick-guide-to-understanding-rabbitmq-amqp-ba25fdfe421d

Run:  python examples/14_amqp/demo.py
"""

from __future__ import annotations

import socket
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402


# See: https://www.rabbitmq.com/tutorials/tutorial-five-python (topic exchange rules)
def topic_matches(pattern: str, key: str) -> bool:
    """AMQP topic matching: ``*`` = exactly one word, ``#`` = zero or more words."""
    def m(p: list[str], k: list[str]) -> bool:
        if not p:
            return not k
        if p[0] == "#":
            return any(m(p[1:], k[i:]) for i in range(len(k) + 1))
        return bool(k) and (p[0] in ("*", k[0])) and m(p[1:], k[1:])
    return m(pattern.split("."), key.split("."))


@dataclass
class Binding:
    """Link between an exchange and a queue."""

    queue: str
    key: str = ""
    args: dict[str, Any] = field(default_factory=dict)


class MiniBroker:
    """Tiny AMQP-like broker implementing the five exchange types."""

    def __init__(self) -> None:
        """Pre-declare the default exchange (as real brokers do)."""
        self.exchanges: dict[str, str] = {"": "direct"}
        self.bindings: dict[str, list[Binding]] = defaultdict(list)
        self.queues: dict[str, deque[tuple[str, str]]] = {}

    def exchange_declare(self, name: str, kind: str) -> None:
        """Declare an exchange (applications declare what they need - 'programmable protocol')."""
        self.exchanges[name] = kind

    def queue_declare(self, name: str) -> None:
        """Declare a queue; it is auto-bound to the default exchange by its name."""
        self.queues.setdefault(name, deque())
        self.bindings[""].append(Binding(name, name))

    def queue_bind(self, queue: str, exchange: str, key: str = "", **args: Any) -> None:
        """Bind a queue to an exchange with a binding key / header arguments."""
        self.bindings[exchange].append(Binding(queue, key, args))

    def _route(self, exchange: str, key: str, headers: dict[str, Any]) -> set[str]:
        kind = self.exchanges[exchange]
        out: set[str] = set()
        for b in self.bindings[exchange]:
            if kind == "fanout":
                ok = True
            elif kind == "direct":
                ok = b.key == key
            elif kind == "topic":
                ok = topic_matches(b.key, key)
            else:  # headers
                wanted = {k: v for k, v in b.args.items() if k != "x-match"}
                hits = [headers.get(k) == v for k, v in wanted.items()]
                ok = all(hits) if b.args.get("x-match", "all") == "all" else any(hits)
            if ok:
                out.add(b.queue)
        return out

    def basic_publish(self, exchange: str, routing_key: str, body: str, headers: dict[str, Any] | None = None) -> set[str]:
        """Publish a message; returns the queues that received a copy."""
        targets = self._route(exchange, routing_key, headers or {})
        for q in targets:
            self.queues[q].append((routing_key, body))
        shown = f"'{exchange}'" if exchange else "(default)"
        log("producer", f"-> exchange {shown:<10} key={routing_key!r:<24} routed to {sorted(targets) or 'NOBODY (dropped)'}")
        return targets

    def drain(self, queue: str) -> list[str]:
        """Consume all messages in a queue."""
        msgs = [b for _, b in self.queues[queue]]
        self.queues[queue].clear()
        return msgs


def mini_broker_demo() -> None:
    """Show the five exchange types with the in-process broker."""
    b = MiniBroker()
    for q in ("tasks", "q.bilbao", "q.spain", "q.all", "q.storms", "q.dash", "q.archive", "q.pdf-or-eu"):
        b.queue_declare(q)

    section("Default exchange: publish 'to a queue' by using its name as routing key")
    b.basic_publish("", "tasks", "recompute averages")

    section("Direct exchange (unicast by exact key)")
    b.exchange_declare("readings.direct", "direct")
    b.queue_bind("q.bilbao", "readings.direct", "bilbao")
    b.basic_publish("readings.direct", "bilbao", "19.2")
    b.basic_publish("readings.direct", "oslo", "9.1")

    section("Fanout exchange (broadcast, routing key ignored)")
    b.exchange_declare("broadcast", "fanout")
    for q in ("q.dash", "q.archive"):
        b.queue_bind(q, "broadcast")
    b.basic_publish("broadcast", "whatever", "system maintenance at 02:00")

    section("Topic exchange (pub/sub with patterns: * one word, # zero or more)")
    b.exchange_declare("weather", "topic")
    b.queue_bind("q.spain", "weather", "es.*.temperature")
    b.queue_bind("q.all", "weather", "#")
    b.queue_bind("q.storms", "weather", "*.*.alert.storm")
    b.basic_publish("weather", "es.bilbao.temperature", "19.2")
    b.basic_publish("weather", "no.oslo.temperature", "9.1")
    b.basic_publish("weather", "es.bilbao.alert.storm", "wind 90 km/h")

    section("Headers exchange (route on message headers, x-match all/any)")
    b.exchange_declare("reports", "headers")
    b.queue_bind("q.pdf-or-eu", "reports", **{"x-match": "any", "format": "pdf", "region": "eu"})
    b.basic_publish("reports", "", "monthly.pdf", headers={"format": "pdf", "region": "us"})
    b.basic_publish("reports", "", "weekly.csv", headers={"format": "csv", "region": "asia"})

    section("Consumers drain their queues")
    for q in b.queues:
        log(f"consumer:{q}", str(b.drain(q)))


def rabbitmq_available(host: str = "127.0.0.1", port: int = 5672) -> bool:
    """True if something listens on the AMQP port."""
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def real_rabbitmq_demo() -> None:
    """Topic exchange scenario against a real RabbitMQ using pika."""
    section("Part 2: the same topic routing on a REAL RabbitMQ broker (pika)")
    try:
        import pika
    except ImportError:
        log("rabbitmq", "pika not installed -> skipping part 2 (pip install pika)")
        return
    if not rabbitmq_available():
        log("rabbitmq", "no broker on localhost:5672 -> skipping part 2 (see docker command in docstring)")
        return
    # See: https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()
    ch.exchange_declare("unit0.weather", exchange_type="topic", auto_delete=True)
    queues = {}
    for pattern in ("es.*.temperature", "#.storm"):
        q = ch.queue_declare("", exclusive=True).method.queue  # server-named, deleted on close
        ch.queue_bind(q, "unit0.weather", routing_key=pattern)
        queues[pattern] = q
    for key, body in [("es.bilbao.temperature", "19.2"), ("no.oslo.temperature", "9.1"), ("es.bilbao.alert.storm", "90km/h")]:
        ch.basic_publish("unit0.weather", key, body.encode())
        log("pika-producer", f"published {key} -> {body}")
    time.sleep(0.3)
    for pattern, q in queues.items():
        got = []
        while (m := ch.basic_get(q, auto_ack=True))[0]:
            got.append(m[2].decode())
        log("pika-consumer", f"binding {pattern!r:<20} received {got}")
    conn.close()


def main() -> None:
    """Run both parts."""
    banner("14 · AMQP: exchanges, bindings, queues (RabbitMQ model)", "64-71")
    section("Part 1: in-process mini-broker implementing the AMQP routing rules")
    mini_broker_demo()
    real_rabbitmq_demo()
    takeaway(
        "Producers publish to EXCHANGES; bindings decide which QUEUES get a copy.",
        "One protocol gives point-to-point (direct/default) AND pub/sub (fanout/topic/headers).",
        "Applications declare their own entities: AMQP is a 'programmable protocol'.",
        "Messages with no matching binding are dropped (unless an alternate exchange is configured).",
    )


if __name__ == "__main__":
    main()
