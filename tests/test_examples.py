"""Smoke tests: every example must finish successfully (or be skipped for missing deps).

Run with:  pytest -q        (or: pytest -q -k raft)
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEMOS = sorted((ROOT / "examples").glob("*/demo.py"))
SKIP_EXIT_CODE = 77


@pytest.mark.parametrize("demo", DEMOS, ids=[d.parent.name for d in DEMOS])
def test_example_runs(demo: Path) -> None:
    """Run one demo in a subprocess and check its exit code."""
    proc = subprocess.run([sys.executable, str(demo)], cwd=ROOT, capture_output=True, text=True,
                          timeout=120, env={**os.environ, "PYTHONUNBUFFERED": "1"})
    if proc.returncode == SKIP_EXIT_CODE:
        pytest.skip(proc.stdout.strip().splitlines()[-1])
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    assert "Key takeaways" in proc.stdout
