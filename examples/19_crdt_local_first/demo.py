"""19 · CRDTs and local-first sync  [NEW].

**Conflict-free Replicated Data Types** let every replica accept writes
*while offline*, with no leader and no consensus, and still guarantee that all
replicas **converge** to the same state once they exchange state - in any order,
any number of times. Their ``merge`` is commutative, associative and idempotent.

They power collaborative/local-first software (Figma-like multiplayer, Apple Notes,
Automerge, Yjs, Redis Enterprise active-active, Riak, Azure Cosmos DB...), and are
the decentralised answer to the central sequencer of example 09.

We implement three classic state-based CRDTs used by a weather app on three devices:

* **PN-Counter**   - number of readings reported (increments/decrements per replica)
* **LWW-Map**      - latest temperature per city (last-writer-wins by (timestamp, replica))
* **OR-Set**       - favourite cities (observed-remove set: concurrent add beats remove)

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install pycrdt          # Python bindings of Yrs (the Rust port of Yjs)

Tutorials & references:
    - crdt.tech: about CRDTs
      https://crdt.tech/
    - crdt.tech: papers
      https://crdt.tech/papers.html
    - Ink & Switch: Local-first software (2019)
      https://www.inkandswitch.com/local-first/
    - Automerge
      https://automerge.org/
    - Yjs documentation
      https://docs.yjs.dev/
    - pycrdt documentation
      https://y-crdt.github.io/pycrdt/

Run:  python examples/19_crdt_local_first/demo.py
"""

from __future__ import annotations

import itertools
import json
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402


@dataclass
class PNCounter:
    """Counter as two per-replica vectors: P (increments) and N (decrements)."""

    p: dict[str, int] = field(default_factory=dict)
    n: dict[str, int] = field(default_factory=dict)

    def inc(self, rid: str, k: int = 1) -> None:
        """Increment (or decrement if ``k`` < 0) on replica ``rid``."""
        target = self.p if k >= 0 else self.n
        target[rid] = target.get(rid, 0) + abs(k)

    def value(self) -> int:
        """Current value."""
        return sum(self.p.values()) - sum(self.n.values())

    def merge(self, o: "PNCounter") -> None:
        """Element-wise max of both vectors."""
        for mine, theirs in ((self.p, o.p), (self.n, o.n)):
            for r, v in theirs.items():
                mine[r] = max(mine.get(r, 0), v)


@dataclass
class LWWMap:
    """Map where each key keeps the value with the highest (timestamp, replica) stamp."""

    items: dict[str, tuple[float, tuple[int, str]]] = field(default_factory=dict)

    def set(self, key: str, value: float, clock: int, rid: str) -> None:
        """Write ``value`` with a logical timestamp."""
        self.merge(LWWMap({key: (value, (clock, rid))}))

    def merge(self, o: "LWWMap") -> None:
        """Keep, per key, the entry with the largest stamp."""
        for k, (v, stamp) in o.items.items():
            if k not in self.items or tuple(stamp) > tuple(self.items[k][1]):
                self.items[k] = (v, tuple(stamp))  # type: ignore[assignment]

    def value(self) -> dict[str, float]:
        """Plain dict view."""
        return {k: v for k, (v, _) in sorted(self.items.items())}


@dataclass
# See: https://crdt.tech/ and Shapiro et al. (OR-Set), https://crdt.tech/papers.html
class ORSet:
    """Observed-Remove Set: each add gets a unique tag; remove deletes only observed tags."""

    adds: dict[str, set[str]] = field(default_factory=dict)
    removes: dict[str, set[str]] = field(default_factory=dict)

    def add(self, e: str) -> None:
        """Add an element with a fresh unique tag."""
        self.adds.setdefault(e, set()).add(uuid.uuid4().hex[:6])

    def remove(self, e: str) -> None:
        """Tombstone the tags we have *seen* for ``e``."""
        self.removes.setdefault(e, set()).update(self.adds.get(e, set()))

    def value(self) -> set[str]:
        """Elements with at least one tag that was not removed."""
        return {e for e, tags in self.adds.items() if tags - self.removes.get(e, set())}

    def merge(self, o: "ORSet") -> None:
        """Union of adds and of removes."""
        for mine, theirs in ((self.adds, o.adds), (self.removes, o.removes)):
            for e, tags in theirs.items():
                mine.setdefault(e, set()).update(tags)


class Replica:
    """A device holding the three CRDTs; works fully offline."""

    def __init__(self, rid: str) -> None:
        """Create empty state."""
        self.rid, self.clock = rid, 0
        self.count, self.temps, self.favs = PNCounter(), LWWMap(), ORSet()

    def report(self, city: str, t: float) -> None:
        """Local write: new reading."""
        self.clock += 1
        self.temps.set(city, t, self.clock, self.rid)
        self.count.inc(self.rid)
        log(self.rid, f"(offline) report {city}={t}")

    def export(self) -> str:
        """Serialise the full state (what would travel over the network)."""
        return json.dumps({"clock": self.clock, "p": self.count.p, "n": self.count.n,
                           "temps": self.temps.items,
                           "adds": {k: sorted(v) for k, v in self.favs.adds.items()},
                           "removes": {k: sorted(v) for k, v in self.favs.removes.items()}})

    def merge_from(self, payload: str) -> None:
        """Merge a remote state (order and repetition don't matter)."""
        d = json.loads(payload)
        self.clock = max(self.clock, d["clock"])  # Lamport-style clock catch-up
        self.count.merge(PNCounter(d["p"], d["n"]))
        self.temps.merge(LWWMap({k: (v, tuple(s)) for k, (v, s) in d["temps"].items()}))
        self.favs.merge(ORSet({k: set(v) for k, v in d["adds"].items()}, {k: set(v) for k, v in d["removes"].items()}))

    def view(self) -> dict[str, Any]:
        """User-visible state."""
        return {"readings": self.count.value(), "temps": self.temps.value(), "favs": sorted(self.favs.value())}


def main() -> None:
    """Run the offline-edit and sync story."""
    banner("19 · CRDTs: offline edits that always converge (local-first)", added=True)
    phone, laptop, cloud = Replica("phone"), Replica("laptop"), Replica("cloud")

    section("Shared starting point (synced once)")
    cloud.report("bilbao", 18.0)
    cloud.favs.add("bilbao")
    for r in (phone, laptop):
        r.merge_from(cloud.export())
    log("all", f"{phone.view()}")

    section("Network is DOWN: every device keeps working and edits concurrently")
    phone.report("bilbao", 19.5)
    laptop.report("bilbao", 17.9)  # concurrent write to the same key!
    laptop.report("madrid", 26.1)
    phone.favs.add("oslo")
    laptop.favs.remove("bilbao")  # laptop removes bilbao...
    phone.favs.add("bilbao")      # ...while phone re-adds it concurrently
    for r in (phone, laptop, cloud):
        log(r.rid, f"local view: {r.view()}")

    section("Network is back: gossip states in an arbitrary order (even duplicated)")
    states = {r.rid: r.export() for r in (phone, laptop, cloud)}
    log("net", f"state payload sizes (bytes): { {k: len(v) for k, v in states.items()} }")
    laptop.merge_from(states["phone"])
    cloud.merge_from(states["laptop"])
    cloud.merge_from(states["phone"])
    phone.merge_from(cloud.export())
    laptop.merge_from(cloud.export())
    laptop.merge_from(cloud.export())  # idempotent: merging twice changes nothing
    for r in (phone, laptop, cloud):
        log(r.rid, f"converged view: {r.view()}")
    log("check", f"all replicas equal: {phone.view() == laptop.view() == cloud.view()}")
    log("why", "temps: phone & laptop wrote bilbao at the same logical time -> deterministic tie-break "
               "by replica id; nobody's 'madrid' write was lost")
    log("why", "favs: 'bilbao' survives because phone's concurrent add carries a tag laptop never observed (add-wins)")
    log("why", "readings: 1 + 1 (phone) + 2 (laptop) = 4, no double counting despite duplicate merges")

    section("Why it works: merge is commutative + associative + idempotent (all 6 orders tested)")
    results = set()
    for order in itertools.permutations(states.values()):
        r = Replica("probe")
        for st in order:
            r.merge_from(st)
        results.add(json.dumps(r.view(), sort_keys=True))
    log("check", f"distinct results over all merge orders: {len(results)}")

    takeaway(
        "No leader, no locks, no consensus: writes are always local and instant (high availability).",
        "Convergence is guaranteed by math: merge is a join (commutative, associative, idempotent).",
        "Semantics are chosen per type: LWW for registers, add-wins for sets, per-replica vectors for counters.",
        "Trade-off: eventual (not strong) consistency. Libraries: Automerge, Yjs, Loro; DBs: Riak, Redis CRDTs.",
    )


if __name__ == "__main__":
    main()
