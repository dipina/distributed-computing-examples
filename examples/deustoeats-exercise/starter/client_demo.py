#!/usr/bin/env python3
"""A tiny client (GIVEN): places an order through the REST API and follows it with Server-Sent Events.

    python client_demo.py                          # 2 tortillas + 1 café for student s001
    python client_demo.py s042 bocadillo-jamon 2   # custom order
"""

from __future__ import annotations

import json
import sys
import uuid

import httpx

from common import API_PORT

API = f"http://127.0.0.1:{API_PORT}"


def main() -> None:
    """POST an order, then stream its status changes until READY."""
    student, sku, qty = (sys.argv[1:4] + [None] * 3)[:3]
    lines = [{"sku": sku, "qty": int(qty)}] if sku else [{"sku": "pintxo-tortilla", "qty": 2}, {"sku": "cafe", "qty": 1}]
    body = {"student_id": student or "s001", "lines": lines}
    r = httpx.post(f"{API}/orders", json=body, headers={"Idempotency-Key": uuid.uuid4().hex})
    print(f"POST /orders -> {r.status_code} Location={r.headers.get('location')}\n{json.dumps(r.json(), indent=2)}")
    if r.status_code != 201:
        return
    with httpx.stream("GET", f"{API}{r.json()['links']['events']}", timeout=60) as s:
        for line in s.iter_lines():
            if line.startswith("data: "):
                print("SSE:", line[6:])


if __name__ == "__main__":
    main()
