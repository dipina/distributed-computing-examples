#!/usr/bin/env python3
"""Run every example (or a selection) and print a summary table.

Usage:
    python run_all.py                 # run all 24 examples in order
    python run_all.py --list          # list the examples
    python run_all.py 02 10 18        # run only those (by number...)
    python run_all.py raft grpc       # ...or by name fragment
    python run_all.py --skip 12 13    # everything except gRPC and real-time web
    python run_all.py --quiet         # only show the summary table
    python run_all.py --classic       # only the paradigms in the slides (01-16)
    python run_all.py --new           # only the cutting-edge additions (17-23)
    python run_all.py --capstone      # only the integrated capstone platform (24)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXAMPLES = ROOT / "examples"
SKIP_EXIT_CODE = 77
FIRST_NEW = 17
CAPSTONE = 24


@dataclass
class Result:
    """Outcome of running one example."""

    name: str
    status: str
    seconds: float
    detail: str = ""


def discover() -> list[Path]:
    """Return the ``demo.py`` of every example folder, sorted by number."""
    return sorted(p / "demo.py" for p in EXAMPLES.iterdir() if (p / "demo.py").exists())


def matches(demo: Path, selectors: list[str]) -> bool:
    """True if the example folder matches any selector (number or name fragment)."""
    folder = demo.parent.name
    num = folder.split("_", 1)[0]
    return any(s.zfill(2) == num or s.lower() in folder.lower() for s in selectors)


def run(demo: Path, timeout: float, quiet: bool) -> Result:
    """Run one demo in a fresh interpreter and classify the result."""
    name = demo.parent.name
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            [sys.executable, str(demo)],
            cwd=ROOT,
            env=env,
            timeout=timeout,
            stdout=subprocess.PIPE if quiet else None,
            stderr=subprocess.STDOUT if quiet else None,
            text=True,
        )
    except subprocess.TimeoutExpired:
        return Result(name, "TIMEOUT", time.perf_counter() - t0)
    dt = time.perf_counter() - t0
    out = proc.stdout or ""
    if proc.returncode == 0:
        return Result(name, "PASS", dt)
    if proc.returncode == SKIP_EXIT_CODE:
        reason = next((l for l in out.splitlines() if l.startswith("SKIPPED")), "optional dependency missing")
        return Result(name, "SKIP", dt, reason)
    tail = "\n".join(out.splitlines()[-15:]) if quiet else ""
    return Result(name, "FAIL", dt, f"exit code {proc.returncode}\n{tail}")


def main() -> int:
    """Parse arguments, run the selected examples, print the summary."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("only", nargs="*", help="numbers or name fragments to run")
    ap.add_argument("--skip", nargs="*", default=[], help="numbers or name fragments to skip")
    ap.add_argument("--list", action="store_true", help="list examples and exit")
    ap.add_argument("--quiet", "-q", action="store_true", help="hide demo output, show summary only")
    ap.add_argument("--timeout", type=float, default=90.0, help="per-example timeout in seconds")
    ap.add_argument("--fail-fast", action="store_true", help="stop at the first failure")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--classic", action="store_true", help="only examples 01-16 (in the slides)")
    grp.add_argument("--new", action="store_true", help="only examples 17-23 (cutting-edge additions)")
    grp.add_argument("--capstone", action="store_true", help="only example 24 (integrated platform)")
    args = ap.parse_args()

    demos = discover()
    if args.only:
        demos = [d for d in demos if matches(d, args.only)]
    if args.skip:
        demos = [d for d in demos if not matches(d, args.skip)]
    num = lambda d: int(d.parent.name[:2])  # noqa: E731
    if args.classic:
        demos = [d for d in demos if num(d) < FIRST_NEW]
    elif args.new:
        demos = [d for d in demos if FIRST_NEW <= num(d) < CAPSTONE]
    elif args.capstone:
        demos = [d for d in demos if num(d) == CAPSTONE]

    if args.list:
        for d in demos:
            tag = " (CAPSTONE)" if num(d) == CAPSTONE else " (NEW)" if num(d) >= FIRST_NEW else ""
            print(f"  {d.parent.name}{tag}")
        return 0
    if not demos:
        print("No example matches the selection. Use --list.")
        return 2

    results: list[Result] = []
    for d in demos:
        if not args.quiet:
            print(f"\n\n######## {d.parent.name} ########\n", flush=True)
        else:
            print(f"running {d.parent.name} ...", flush=True)
        r = run(d, args.timeout, args.quiet)
        results.append(r)
        if args.fail_fast and r.status in {"FAIL", "TIMEOUT"}:
            break

    print("\n" + "=" * 78 + "\n SUMMARY\n" + "=" * 78)
    for r in results:
        print(f"  {r.status:<8} {r.seconds:6.1f}s  {r.name}")
        if r.detail and r.status != "PASS":
            for line in r.detail.splitlines():
                print(f"{'':20}{line}")
    counts = {s: sum(r.status == s for r in results) for s in ("PASS", "SKIP", "FAIL", "TIMEOUT")}
    print("-" * 78)
    print("  " + "  ".join(f"{k}={v}" for k, v in counts.items()))
    return 1 if counts["FAIL"] or counts["TIMEOUT"] else 0


if __name__ == "__main__":
    sys.exit(main())
