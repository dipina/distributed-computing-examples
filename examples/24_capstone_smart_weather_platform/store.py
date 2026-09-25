"""Shared storage of the capstone: the partitioned commit log (write side) and the read model (query side).

* :class:`CommitLog` - Kafka-style, file-backed, append-only log (see example 17).
  Exactly ONE process writes it (the ingest service); many processes read it.
* :class:`ReadModel` - SQLite database holding the materialised views (CQRS read side),
  the alerts and the consumer-group offsets. Updating the view AND committing the
  offset in the same transaction gives effectively-once processing.

References:
    - SQLite WAL: https://www.sqlite.org/wal.html
    - sqlite3 module: https://docs.python.org/3/library/sqlite3.html
    - M. Fowler: CQRS: https://martinfowler.com/bliki/CQRS.html
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

PARTITIONS = 3
GROUP = "analytics"
ALERT_LEVELS = ((40.0, "RED"), (35.0, "ORANGE"))


def partition_for(key: str) -> int:
    """Stable key -> partition mapping (same city, same partition, per-city ordering)."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % PARTITIONS


def alert_level(celsius: float) -> str | None:
    """Return the alert level for a temperature, or None."""
    return next((lvl for threshold, lvl in ALERT_LEVELS if celsius >= threshold), None)


class CommitLog:
    """Partitioned append-only log stored as JSON-lines files."""

    def __init__(self, data_dir: Path, topic: str = "readings") -> None:
        """Open (and create) the partition files."""
        self.dir = data_dir / "log"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.files = [self.dir / f"{topic}-{p}.log" for p in range(PARTITIONS)]
        for f in self.files:
            f.touch()
        self._next = [self.end_offset(p) for p in range(PARTITIONS)]

    def append(self, key: str, value: dict[str, Any]) -> tuple[int, int]:
        """Append a record (single-writer). Returns (partition, offset)."""
        p = partition_for(key)
        offset = self._next[p]
        with self.files[p].open("a") as fh:
            fh.write(json.dumps({"offset": offset, "key": key, "value": value}) + "\n")
        self._next[p] += 1
        return p, offset

    def read(self, partition: int, offset: int, max_records: int) -> list[dict[str, Any]]:
        """Read up to ``max_records`` complete records starting at ``offset``."""
        with self.files[partition].open() as fh:
            lines = fh.readlines()
        return [json.loads(l) for l in lines[offset:offset + max_records] if l.endswith("\n")]

    def end_offset(self, partition: int) -> int:
        """Number of records in a partition."""
        with self.files[partition].open() as fh:
            return sum(1 for l in fh if l.endswith("\n"))


class ReadModel:
    """SQLite-backed CQRS read model + offsets + alerts."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS stats (city TEXT PRIMARY KEY, count INTEGER, total REAL,
                                      min REAL, max REAL, last REAL, updated_ms INTEGER);
    CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, celsius REAL,
                                       level TEXT, ts_ms INTEGER, partition INTEGER, offset INTEGER,
                                       UNIQUE(partition, offset));
    CREATE TABLE IF NOT EXISTS offsets (grp TEXT, partition INTEGER, next_offset INTEGER,
                                        PRIMARY KEY (grp, partition));
    """

    def __init__(self, data_dir: Path) -> None:
        """Connect (WAL mode lets readers and one writer work concurrently)."""
        self.db = sqlite3.connect(data_dir / "readmodel.db", timeout=10, isolation_level=None,
                                  check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # See: https://www.sqlite.org/wal.html
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(self.SCHEMA)

    # ------------------------------------------------------------ write side (analytics workers)
    def committed(self, group: str, partition: int) -> int:
        """Next offset to process for (group, partition)."""
        row = self.db.execute("SELECT next_offset FROM offsets WHERE grp=? AND partition=?",
                              (group, partition)).fetchone()
        return row[0] if row else 0

    # See: https://microservices.io/patterns/data/transactional-outbox.html (same atomicity idea)
    def apply_batch(self, group: str, partition: int, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Update views, record alerts and commit the offset - atomically.

        Returns:
            The alerts raised by this batch.
        """
        alerts = []
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for r in records:
                city, c, ts = r["key"], r["value"]["celsius"], r["value"]["ts_ms"]
                self.db.execute(
                    """INSERT INTO stats VALUES (?, 1, ?, ?, ?, ?, ?)
                       ON CONFLICT(city) DO UPDATE SET count=count+1, total=total+excluded.total,
                       min=MIN(min, excluded.min), max=MAX(max, excluded.max),
                       last=excluded.last, updated_ms=excluded.updated_ms""",
                    (city, c, c, c, c, ts))
                if level := alert_level(c):
                    self.db.execute("INSERT OR IGNORE INTO alerts (city, celsius, level, ts_ms, partition, offset) "
                                    "VALUES (?, ?, ?, ?, ?, ?)", (city, c, level, ts, partition, r["offset"]))
                    alerts.append({"city": city, "celsius": c, "level": level})
            self.db.execute("INSERT INTO offsets VALUES (?, ?, ?) ON CONFLICT(grp, partition) "
                            "DO UPDATE SET next_offset=excluded.next_offset",
                            (group, partition, records[-1]["offset"] + 1))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        return alerts

    # ------------------------------------------------------------ read side (API)
    def stations(self) -> list[dict[str, Any]]:
        """All station statistics."""
        rows = self.db.execute("SELECT * FROM stats ORDER BY city").fetchall()
        return [self._stat(r) for r in rows]

    def station(self, city: str) -> dict[str, Any] | None:
        """Statistics of one city."""
        r = self.db.execute("SELECT * FROM stats WHERE city=?", (city,)).fetchone()
        return self._stat(r) if r else None

    @staticmethod
    def _stat(r: sqlite3.Row) -> dict[str, Any]:
        return {"city": r["city"], "count": r["count"], "mean": round(r["total"] / r["count"], 2),
                "min": r["min"], "max": r["max"], "last": r["last"]}

    def alerts(self, after_id: int = 0, city: str | None = None, level: str | None = None,
               limit: int = 50) -> list[dict[str, Any]]:
        """Alerts newer than ``after_id``, optionally filtered."""
        q, args = "SELECT id, city, celsius, level, ts_ms FROM alerts WHERE id > ?", [after_id]
        if city:
            q, args = q + " AND city = ?", [*args, city]
        if level:
            q, args = q + " AND level = ?", [*args, level]
        rows = self.db.execute(q + " ORDER BY id LIMIT ?", (*args, limit)).fetchall()
        return [dict(r) for r in rows]

    def pipeline(self, log: CommitLog) -> list[dict[str, int]]:
        """Consumer lag per partition (observability)."""
        out = []
        for p in range(PARTITIONS):
            end, done = log.end_offset(p), self.committed(GROUP, p)
            out.append({"partition": p, "end_offset": end, "committed": done, "lag": end - done})
        return out


def now_ms() -> int:
    """Wall-clock time in milliseconds."""
    return int(time.time() * 1000)
