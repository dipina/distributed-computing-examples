#!/usr/bin/env python3
"""Automatic acceptance tests for DeustoEats (GIVEN - do not change; the teacher runs the same file).

Start the system first (python run_local.py) or simply run:   python run_local.py --check
The tests are BLACK-BOX: they only use the public REST API (and the notifier's output file).
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import httpx

from common import API_PORT, NOTIFICATIONS_FILE

API = f"http://127.0.0.1:{API_PORT}"
C = httpx.Client(base_url=API, timeout=10)
CREATED: list[str] = []        # ids of orders created by the tests (used by later tests)
RESULTS: list[tuple[str, float, float, str]] = []


def order(student: str, lines: list[tuple[str, int]], key: str | None = None) -> httpx.Response:
    """POST /orders helper."""
    headers = {"Idempotency-Key": key} if key else {}
    return C.post("/orders", json={"student_id": student, "lines": [{"sku": s, "qty": q} for s, q in lines]},
                  headers=headers)


def wait_ready(ids: list[str], timeout: float = 45) -> list[dict[str, Any]]:
    """Poll until all orders are READY (or time out)."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        got = [C.get(f"/orders/{i}").json() for i in ids]
        if all(o["status"] == "READY" for o in got):
            return got
        time.sleep(0.5)
    raise AssertionError(f"not all READY after {timeout}s: {[(o['id'], o['status']) for o in got]}")


# ------------------------------------------------------------------------------------------ tests
def t01_health() -> None:
    """GET /health answers 200."""
    assert C.get("/health").status_code == 200


def t02_create() -> None:
    """POST /orders: 201, Location header, PENDING, total priced by the menu-service (620 cents)."""
    r = order("s100", [("pintxo-tortilla", 2), ("cafe", 1)])
    assert r.status_code == 201, f"expected 201, got {r.status_code}: {r.text}"
    b = r.json()
    assert r.headers.get("location") == f"/orders/{b['id']}", f"Location header: {r.headers.get('location')}"
    assert b["total_cents"] == 2 * 250 + 120, f"total_cents={b.get('total_cents')}, expected 620"
    assert b["status"] in ("PENDING", "COOKING", "READY") and b["student_id"] == "s100"
    assert b["links"]["events"] == f"/orders/{b['id']}/events"
    CREATED.append(b["id"])


def t03_get() -> None:
    """GET /orders/{id} returns the order; an unknown id returns 404."""
    oid = CREATED[0]
    r = C.get(f"/orders/{oid}")
    assert r.status_code == 200 and r.json()["id"] == oid
    assert C.get("/orders/does-not-exist").status_code == 404


def t04_validation() -> None:
    """Invalid bodies are rejected with 422 (Pydantic) BEFORE calling other services."""
    assert order("s100", [("cafe", 0)]).status_code == 422, "qty 0 must be 422"
    assert order("s100", []).status_code == 422, "empty lines must be 422"
    assert C.post("/orders", json={"lines": [{"sku": "cafe", "qty": 1}]}).status_code == 422, "missing student_id"


def t05_grpc_errors() -> None:
    """gRPC status codes of menu-service are mapped to HTTP: NOT_FOUND->404, FAILED_PRECONDITION->409."""
    r = order("s100", [("paella", 1)])
    assert r.status_code == 404, f"unknown sku: expected 404, got {r.status_code}"
    r = order("s100", [("gilda", 1)])
    assert r.status_code == 409, f"sold-out item: expected 409, got {r.status_code}"
    r = order("s100", [("ensalada", 6)])
    assert r.status_code == 409, f"qty > stock: expected 409, got {r.status_code}"


def t06_idempotency() -> None:
    """Same Idempotency-Key twice -> same order (201 then 200) and only ONE order is created."""
    key, student = uuid.uuid4().hex, f"s-idem-{uuid.uuid4().hex[:4]}"
    r1 = order(student, [("cafe", 1)], key)
    r2 = order(student, [("cafe", 1)], key)
    assert r1.status_code == 201 and r2.status_code == 200, f"got {r1.status_code} then {r2.status_code}"
    assert r1.json()["id"] == r2.json()["id"], "the retry created a different order"
    assert len(C.get("/orders", params={"student_id": student}).json()) == 1, "duplicate order stored"
    CREATED.append(r1.json()["id"])


def t07_sse() -> None:
    """GET /orders/{id}/events streams the status changes in order and ends after READY."""
    oid = order("s200", [("bocadillo-jamon", 1)]).json()["id"]
    CREATED.append(oid)
    seen: list[str] = []
    with httpx.stream("GET", f"{API}/orders/{oid}/events", timeout=30) as s:
        assert s.headers["content-type"].startswith("text/event-stream"), s.headers["content-type"]
        for line in s.iter_lines():
            if line.startswith("data: "):
                seen.append(json.loads(line[6:])["status"])
    assert seen and seen[-1] == "READY", f"stream did not end with READY: {seen}"
    order_idx = [["PENDING", "COOKING", "READY"].index(x) for x in seen]
    assert order_idx == sorted(order_idx) and len(set(seen)) == len(seen), f"bad sequence {seen}"


def t08_work_queue() -> None:
    """6 concurrent orders are all cooked; the orders were shared by at least 2 kitchen workers."""
    menu = [("pintxo-tortilla", 1), ("cafe", 2), ("bocadillo-jamon", 1), ("ensalada", 1), ("cafe", 1), ("pintxo-tortilla", 2)]
    with ThreadPoolExecutor(6) as pool:
        rs = list(pool.map(lambda i: order(f"s3{i:02d}", [menu[i]]), range(6)))
    assert all(r.status_code == 201 for r in rs), [r.status_code for r in rs]
    ids = [r.json()["id"] for r in rs]
    CREATED.extend(ids)
    wait_ready(ids)
    workers = {o["cooked_by"] for o in wait_ready(CREATED)}     # all orders so far (worker-2 may have crashed)
    assert len(workers) >= 2, f"only {workers} cooked: are two workers consuming the SAME queue?"


def t09_redelivery() -> None:
    """A crashed worker's un-acked order was redelivered and still completed (run_local crashes worker-2)."""
    wait_ready(CREATED)
    redelivered = [oid for oid in CREATED
                   if any(h.get("redelivered") for h in C.get(f"/orders/{oid}").json()["history"])]
    assert redelivered, "no order shows redelivered=true (manual ACK? did worker-2 crash? use run_local.py)"


def t10_filters() -> None:
    """GET /orders?status=READY contains every finished order; an invalid status is 422."""
    ready = {o["id"] for o in C.get("/orders", params={"status": "READY"}).json()}
    missing = set(CREATED) - ready
    assert not missing, f"READY filter misses {missing}"
    assert C.get("/orders", params={"status": "BURNT"}).status_code == 422


def t11_notifier() -> None:
    """The notifier (binding 'order.ready') wrote exactly one SMS line per READY order."""
    assert CREATED, "no orders were created by the previous tests"
    end = time.monotonic() + 5
    text = ""
    while time.monotonic() < end:
        text = NOTIFICATIONS_FILE.read_text(encoding="utf-8") if NOTIFICATIONS_FILE.exists() else ""
        if all(oid in text for oid in CREATED):
            break
        time.sleep(0.5)
    assert text, f"{NOTIFICATIONS_FILE.name} is missing or empty - is notifier.py running?"
    missing = [oid for oid in CREATED if oid not in text]
    assert not missing, f"no notification for {missing} in {NOTIFICATIONS_FILE.name}"
    lines = text.splitlines()
    dup = [oid for oid in CREATED if sum(oid in l for l in lines) != 1]
    assert not dup, f"orders notified more than once {dup}: is the notifier bound ONLY to 'order.ready'?"


TESTS: list[tuple[Callable[[], None], float]] = [
    (t01_health, 0.5), (t02_create, 1.0), (t03_get, 0.5), (t04_validation, 0.5), (t05_grpc_errors, 1.0),
    (t06_idempotency, 0.5), (t07_sse, 1.0), (t08_work_queue, 1.0), (t09_redelivery, 0.5), (t10_filters, 0.5),
    (t11_notifier, 0.5),
]


def main() -> None:
    """Run all tests, print a score table."""
    try:
        C.get("/health")
    except httpx.HTTPError:
        sys.exit(f"orders-api not reachable at {API}: start the system with  python run_local.py")
    total = 0.0
    for fn, pts in TESTS:
        t0 = time.monotonic()
        try:
            fn()
            RESULTS.append((fn.__name__, pts, pts, "PASS"))
            total += pts
        except Exception as exc:  # noqa: BLE001
            RESULTS.append((fn.__name__, 0.0, pts, f"FAIL: {exc}"))
        print(f"{'PASS' if RESULTS[-1][1] else 'FAIL'}  {fn.__name__:<18} {time.monotonic() - t0:5.1f}s  {fn.__doc__}",
              flush=True)
        if not RESULTS[-1][1]:
            print(f"      -> {RESULTS[-1][3][6:]}")
    print("-" * 78)
    print(f"Automatic score: {total:.1f} / {sum(p for _, p in TESTS):.1f}  (+ report and code quality, see README)")
    sys.exit(0 if total == sum(p for _, p in TESTS) else 1)


if __name__ == "__main__":
    main()
