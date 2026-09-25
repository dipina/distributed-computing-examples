"""Component: ANALYTICS worker (member of consumer group "analytics").

Pulls records from its partitions of the commit log, updates the read model,
detects heat alerts and commits its offset - all in ONE SQLite transaction,
so a crash at any point never double-counts nor loses a reading.

    python analytics_worker.py --data DIR --worker 0 --workers 2

References:
    - Kafka consumer groups & offsets: https://kafka.apache.org/documentation/#intro_consumers
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]
from common.utils import log  # noqa: E402
from store import GROUP, PARTITIONS, CommitLog, ReadModel  # noqa: E402

BATCH = 4
WORK_PER_RECORD = 0.04  # simulated processing cost (s)


def main() -> None:
    """Poll the assigned partitions forever."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--worker", type=int, required=True)
    ap.add_argument("--workers", type=int, default=2)
    a = ap.parse_args()
    me = f"analytics-{a.worker}"
    commit_log, rm = CommitLog(a.data), ReadModel(a.data)
    mine = [p for p in range(PARTITIONS) if p % a.workers == a.worker]  # static group assignment
    resume = {p: rm.committed(GROUP, p) for p in mine}
    log(me, f"started, partitions {mine}, resuming from committed offsets {resume}")
    while True:
        idle = True
        for p in mine:
            recs = commit_log.read(p, rm.committed(GROUP, p), BATCH)
            if not recs:
                continue
            idle = False
            time.sleep(WORK_PER_RECORD * len(recs))  # "processing"
            for alert in rm.apply_batch(GROUP, p, recs):
                log(me, f"⚠ {alert['level']} alert: {alert['city']} {alert['celsius']}°C")
            log(me, f"p{p}: processed offsets {recs[0]['offset']}..{recs[-1]['offset']} + committed")
        if idle:
            time.sleep(0.1)


if __name__ == "__main__":
    main()
