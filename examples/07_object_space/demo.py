"""07 · Object space / tuple space (slide 12: "Object Space"; Linda, JavaSpaces).

Processes never talk to each other directly. They coordinate through a shared
associative memory (the *space*) with just three operations:

* ``write(entry)``          - put an entry (a tuple) in the space
* ``read(template)``        - block until a matching entry exists, return a copy
* ``take(template)``        - like read, but *removes* it (atomic -> no double processing)

Templates match by position; ``None`` is a wildcard. Coupling is minimal:
producers and consumers don't know each other's identity, number or lifetime.

Scenario: the classic **master/worker "bag of tasks"** plus a distributed
semaphore implemented as a single token tuple.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)

Tutorials & references:
    - Gelernter, Generative communication in Linda (TOPLAS 1985)
      https://doi.org/10.1145/2363.2433
    - Tuple space (overview)
      https://en.wikipedia.org/wiki/Tuple_space
    - threading.Condition (blocking read/take)
      https://docs.python.org/3/library/threading.html#condition-objects

Run:  python examples/07_object_space/demo.py
"""

from __future__ import annotations

import random
import statistics
import sys
import threading
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import INITIAL_READINGS  # noqa: E402
from common.utils import banner, log, section, takeaway  # noqa: E402

Entry = tuple[Any, ...]


class TupleSpace:
    """A thread-safe Linda-style tuple space."""

    def __init__(self) -> None:
        """Create an empty space."""
        self._entries: list[Entry] = []
        self._cond = threading.Condition()

    @staticmethod
    def _match(template: Entry, entry: Entry) -> bool:
        return len(template) == len(entry) and all(t is None or t == e for t, e in zip(template, entry))

    def write(self, entry: Entry) -> None:
        """Add an entry and wake up waiting readers."""
        with self._cond:
            self._entries.append(entry)
            self._cond.notify_all()

    def _find(self, template: Entry, remove: bool, timeout: float | None) -> Entry | None:
        with self._cond:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                for i, e in enumerate(self._entries):
                    if self._match(template, e):
                        return self._entries.pop(i) if remove else e
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._cond.wait(remaining)

    def read(self, template: Entry, timeout: float | None = None) -> Entry | None:
        """Return (without removing) an entry matching ``template``."""
        return self._find(template, False, timeout)

    def take(self, template: Entry, timeout: float | None = None) -> Entry | None:
        """Atomically remove and return an entry matching ``template``."""
        return self._find(template, True, timeout)

    def __len__(self) -> int:
        with self._cond:
            return len(self._entries)


def worker(space: TupleSpace, name: str) -> None:
    """Take tasks until a poison pill appears; write results back."""
    while True:
        _, tid, city, readings = space.take(("task", None, None, None))  # type: ignore[misc]
        if tid == -1:
            space.write(("task", -1, None, None))  # leave the pill for the other workers
            return
        time.sleep(random.uniform(0.02, 0.1))
        mean = round(statistics.fmean(readings), 2)
        log(name, f"took task {tid} ({city}) -> writes result mean={mean}")
        space.write(("result", tid, city, mean))


def printer_user(space: TupleSpace, name: str) -> None:
    """Uses a shared resource guarded by a token tuple (distributed mutex)."""
    space.take(("printer-token",))
    log(name, "acquired printer token, printing report...")
    time.sleep(0.05)
    log(name, "releasing token")
    space.write(("printer-token",))


def main() -> None:
    """Run the master/worker and mutex scenarios."""
    banner("07 · Object/tuple space (Linda / JavaSpaces)", "12")
    space = TupleSpace()

    section("Master writes a bag of tasks; anonymous workers take them")
    tasks = list(INITIAL_READINGS.items()) * 2
    for tid, (city, readings) in enumerate(tasks):
        space.write(("task", tid, city, readings))
    log("master", f"wrote {len(tasks)} task tuples; space size = {len(space)}")
    workers = [threading.Thread(target=worker, args=(space, f"worker-{i}")) for i in range(3)]
    for w in workers:
        w.start()

    results = [space.take(("result", tid, None, None)) for tid in range(len(tasks))]
    log("master", f"collected {len(results)} results, e.g. {results[0]}")
    space.write(("task", -1, None, None))  # poison pill
    for w in workers:
        w.join()
    space.take(("task", -1, None, None))

    section("read() vs take(): a config tuple read by many, never consumed")
    space.write(("config", "units", "celsius"))
    for n in ("svc-a", "svc-b"):
        log(n, f"read -> {space.read(('config', 'units', None))}")
    log("space", f"config still present: {space.read(('config', None, None), timeout=0.1) is not None}")

    section("A single token tuple = distributed mutual exclusion")
    space.write(("printer-token",))
    users = [threading.Thread(target=printer_user, args=(space, f"user-{i}")) for i in range(3)]
    for u in users:
        u.start()
    for u in users:
        u.join()
    log("space", f"nobody waits for a missing tuple forever: take(('ghost',), timeout=0.2) -> "
                 f"{space.take(('ghost',), timeout=0.2)}")

    takeaway(
        "Coordination through a shared space: decoupled in space (no addresses) and time (entries persist).",
        "take() is atomic, so each task is processed exactly once without any explicit lock.",
        "Load balancing is automatic: faster workers simply take more tasks.",
        "Modern descendants: JavaSpaces/GigaSpaces, Redis/etcd used as coordination spaces.",
    )


if __name__ == "__main__":
    main()
