"""Chaos helper used INSIDE a container: SIGKILL the component's Python process.

    docker compose exec analytics-0 python /app/chaos.py

With ``init: true`` (tini as PID 1) the container then exits with an error and Docker's
restart policy (``restart: unless-stopped``) starts it again - the worker resumes from its
committed offsets. Same idea as the supervisor of example 22/24, done by the container runtime.
"""

from __future__ import annotations

import os
import signal
from pathlib import Path

COMPONENTS = ("ingest_service.py", "api_service.py", "analytics_worker.py")


def main() -> None:
    """Find the component process in /proc and kill it without any cleanup."""
    me = os.getpid()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == me:
            continue
        try:
            cmd = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if int(proc.name) == 1 or "init" in cmd.split(" ", 1)[0]:
            continue  # skip PID 1 (tini/docker-init): the kernel ignores SIGKILL sent to init from inside
        if "python" in cmd.split(" ", 1)[0] and any(c in cmd for c in COMPONENTS):
            print(f"chaos: SIGKILL pid {proc.name}: {cmd.strip()}")
            os.kill(int(proc.name), signal.SIGKILL)
            return
    print("chaos: no component process found")


if __name__ == "__main__":
    main()
