"""17 · Event streaming with a distributed commit log (Kafka-style)  [NEW].

Not in the slides, but the backbone of modern data platforms (Apache Kafka,
Redpanda, Apache Pulsar, AWS Kinesis, Azure Event Hubs). The key idea: the
broker is an **append-only, persistent, partitioned log**, not a queue that
deletes messages once they are delivered.

* **Partitions** give parallelism; records with the same **key** go to the same
  partition -> per-key ordering.
* Each record has an **offset**. Consumers *pull* and track their own position.
* **Consumer groups**: partitions are split among the members of a group
  (load balancing), while different groups read the SAME data independently (pub/sub).
* Because the log is kept, consumers can **replay** history and **resume** after
  crashes from their committed offset (compare with 04, where late subscribers lost messages).
* **Stream processing** (Kafka Streams / Flink): continuous windowed aggregations.

The log is stored in real files (one per partition) in a temp directory.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Real Kafka: docker run -d --name kafka -p 9092:9092 apache/kafka:latest
                pip install confluent-kafka
    Kafka quickstart: https://kafka.apache.org/quickstart/

Tutorials & references:
    - Apache Kafka quickstart
      https://kafka.apache.org/quickstart/
    - Apache Kafka documentation
      https://kafka.apache.org/documentation/
    - Confluent: Apache Kafka and Python getting started
      https://developer.confluent.io/get-started/python/
    - confluent-kafka Python client
      https://docs.confluent.io/kafka-clients/python/current/overview.html
    - J. Kreps, The Log: what every software engineer should know
      https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
    - Apache Flink (stream processing)
      https://flink.apache.org/

Run:  python examples/17_event_streaming/demo.py
"""

from __future__ import annotations

import json
import sys
import tempfile
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402


class Topic:
    """A partitioned, file-backed, append-only log."""

    def __init__(self, directory: Path, name: str, partitions: int) -> None:
        """Create the partition files."""
        self.name, self.n = name, partitions
        self.files = [directory / f"{name}-{p}.log" for p in range(partitions)]
        for f in self.files:
            f.touch()
        self.committed: dict[tuple[str, int], int] = {}  # (group, partition) -> next offset to read

    def partition_for(self, key: str) -> int:
        """Deterministic key -> partition mapping (Kafka uses murmur2; any stable hash works)."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.n

    def append(self, key: str, value: dict[str, Any]) -> tuple[int, int]:
        """Produce a record. Returns (partition, offset)."""
        p = self.partition_for(key)
        with self.files[p].open("a") as fh:
            offset = sum(1 for _ in self.files[p].open())
            fh.write(json.dumps({"offset": offset, "key": key, "value": value}) + "\n")
        return p, offset

    def read(self, partition: int, offset: int, max_records: int = 100) -> list[dict[str, Any]]:
        """Fetch records from ``offset`` on (the log is never modified by reads)."""
        with self.files[partition].open() as fh:
            lines = fh.readlines()[offset:offset + max_records]
        return [json.loads(line) for line in lines]

    def end_offset(self, partition: int) -> int:
        """Offset of the next record to be written."""
        return sum(1 for _ in self.files[partition].open())


# See: https://kafka.apache.org/documentation/#intro_consumers
class ConsumerGroup:
    """Group coordinator: assigns partitions to members and stores committed offsets."""

    def __init__(self, topic: Topic, group: str) -> None:
        """Create an empty group."""
        self.topic, self.group, self.members = topic, group, []  # type: ignore[var-annotated]

    def join(self, member: str) -> None:
        """Add a member and rebalance."""
        self.members.append(member)
        self._rebalance()

    def leave(self, member: str) -> None:
        """Remove a member (crash or shutdown) and rebalance."""
        self.members.remove(member)
        self._rebalance()

    def _rebalance(self) -> None:
        self.assignment = {m: [p for p in range(self.topic.n) if p % len(self.members) == i]
                           for i, m in enumerate(self.members)}
        log(f"group:{self.group}", f"rebalance -> {self.assignment}")

    def poll(self, member: str, commit: bool = True) -> list[dict[str, Any]]:
        """Fetch new records for the member's partitions, optionally committing offsets."""
        out = []
        for p in self.assignment[member]:
            pos = self.topic.committed.get((self.group, p), 0)
            recs = self.topic.read(p, pos)
            for r in recs:
                r["partition"] = p
            out.extend(recs)
            if commit and recs:
                self.topic.committed[(self.group, p)] = pos + len(recs)
        return out


def fmt(recs: list[dict[str, Any]]) -> str:
    """Compact representation of records."""
    return ", ".join(f"p{r['partition']}@{r['offset']}:{r['key']}={r['value']['c']}" for r in recs) or "(nothing new)"


def main() -> None:
    """Run the event streaming scenarios."""
    banner("17 · Event streaming: partitioned commit log, consumer groups, replay", added=True)
    with tempfile.TemporaryDirectory() as tmp:
        topic = Topic(Path(tmp), "readings", partitions=3)
        log("broker", f"topic 'readings' with 3 partitions stored in {tmp}")

        section("Producers append keyed events; same key -> same partition (per-city ordering)")
        data = [("bilbao", 17.5), ("madrid", 24.1), ("oslo", 8.2), ("bilbao", 18.0), ("madrid", 26.3),
                ("barcelona", 22.4), ("bilbao", 19.2), ("oslo", 7.9)]
        for t, (city, c) in enumerate(data):
            p, off = topic.append(city, {"c": c, "t": t})
            log("producer", f"{city:<9} {c:5} -> partition {p}, offset {off}")

        section("Consumer group 'dashboard' with 2 members shares the partitions")
        dash = ConsumerGroup(topic, "dashboard")
        dash.join("dash-1")
        dash.join("dash-2")
        for m in ("dash-1", "dash-2"):
            log(m, fmt(dash.poll(m)))

        section("An independent group 'analytics' reads the SAME events from offset 0 (replay)")
        ana = ConsumerGroup(topic, "analytics")
        ana.join("ana-1")
        recs = ana.poll("ana-1")
        log("ana-1", f"read {len(recs)} events; history is not lost for new readers")

        section("dash-2 crashes; new events arrive; dash-1 takes over from the COMMITTED offsets")
        dash.leave("dash-2")
        for t, (city, c) in enumerate([("oslo", 9.1), ("barcelona", 23.0), ("madrid", 25.0)], start=len(data)):
            topic.append(city, {"c": c, "t": t})
        log("dash-1", fmt(dash.poll("dash-1")))
        log("dash-1", f"poll again -> {fmt(dash.poll('dash-1'))}")

        section("Stream processing: tumbling-window average per city (like Kafka Streams / Flink)")
        windows: dict[tuple[str, int], list[float]] = defaultdict(list)
        for p in range(topic.n):
            for r in topic.read(p, 0):
                windows[(r["key"], r["value"]["t"] // 4)].append(r["value"]["c"])  # 4-tick windows
        for (city, w), vals in sorted(windows.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            log("stream-job", f"window {w} {city:<9} avg={sum(vals) / len(vals):5.2f} over {len(vals)} events")

        section("Offsets are just numbers per (group, partition): the consumer controls its position")
        log("broker", f"end offsets: {[topic.end_offset(p) for p in range(topic.n)]}, "
                      f"committed: { {f'{g}/p{p}': o for (g, p), o in sorted(topic.committed.items())} }")

    takeaway(
        "The log is persistent and immutable: consuming does NOT delete; many groups read independently.",
        "Partitions = unit of parallelism and ordering (per key). Groups share partitions among members.",
        "Committed offsets make recovery trivial and enable replay/reprocessing of history.",
        "Foundation of event-driven microservices, CDC, event sourcing and real-time analytics.",
    )


if __name__ == "__main__":
    main()
