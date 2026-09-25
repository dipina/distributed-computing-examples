#!/usr/bin/env python3
"""Start the whole DeustoEats system on this machine (GIVEN - you do not need to change this file).

    python run_local.py              # menu-service, orders-api, 2 kitchen workers, notifier  (Ctrl+C to stop)
    python run_local.py --check      # start everything, run check.py, stop everything
    python run_local.py --no-chaos   # worker-2 does NOT crash (by default it crashes on its 3rd order)

Needs a RabbitMQ broker on localhost:5672, e.g.:
    docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from common import API_PORT, KITCHEN_QUEUE, MENU_ADDR, NOTIFICATIONS_FILE, NOTIFIER_QUEUE, rabbit_connection

HERE = Path(__file__).resolve().parent
PY = sys.executable


def wait_port(host: str, port: int, timeout: float = 30) -> None:
    """Block until host:port accepts connections."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return
        except OSError:
            time.sleep(0.2)
    raise SystemExit(f"{host}:{port} did not open within {timeout}s - look at the logs above")


def fresh_broker() -> None:
    """Delete the queues of previous runs so old messages do not leak into this one."""
    conn = rabbit_connection(retries=5)
    ch = conn.channel()
    for q in (KITCHEN_QUEUE, NOTIFIER_QUEUE):
        ch.queue_delete(queue=q)
    conn.close()


def main() -> None:
    """Launch the components as separate OS processes."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="run check.py and exit")
    ap.add_argument("--no-chaos", action="store_true", help="do not crash worker-2")
    a = ap.parse_args()

    fresh_broker()
    NOTIFICATIONS_FILE.unlink(missing_ok=True)
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    host, port = MENU_ADDR.rsplit(":", 1)
    procs = [subprocess.Popen([PY, "menu_service.py"], cwd=HERE, env=env)]
    wait_port(host, int(port))
    procs.append(subprocess.Popen([PY, "orders_api.py"], cwd=HERE, env=env))
    wait_port("127.0.0.1", API_PORT)
    procs.append(subprocess.Popen([PY, "kitchen_worker.py", "--name", "worker-1"], cwd=HERE, env=env))
    procs.append(subprocess.Popen([PY, "kitchen_worker.py", "--name", "worker-2",
                                   *([] if a.no_chaos else ["--crash-after", "3"])], cwd=HERE, env=env))
    procs.append(subprocess.Popen([PY, "notifier.py"], cwd=HERE, env=env))
    time.sleep(1.5)
    print(f"\nDeustoEats is running: http://127.0.0.1:{API_PORT}/docs   (RabbitMQ UI: http://localhost:15672)\n",
          flush=True)
    rc = 0
    try:
        if a.check:
            rc = subprocess.call([PY, "check.py"], cwd=HERE, env=env)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
    sys.exit(rc)


if __name__ == "__main__":
    main()
