"""Component: SENSOR (edge device). Streams readings to the ingest service over gRPC.

    python sensor.py --city sevilla --port 50051 --n 12 --profile heatwave [--host <ingest-host>]

References:
    - gRPC client streaming: https://grpc.io/docs/languages/python/basics/#client-streaming-rpc
    - gRPC metadata: https://grpc.io/docs/guides/metadata/
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path
from typing import Iterator

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]
import grpc  # noqa: E402

from common.utils import log  # noqa: E402
from proto_stubs import pb, pb_grpc  # noqa: E402
from store import now_ms  # noqa: E402

PROFILES = {"cold": (8.0, 0.1), "mild": (17.0, 0.3), "warm": (27.0, 0.5), "heatwave": (32.0, 1.0)}  # (start °C, slope per reading)


def readings(city: str, n: int, profile: str, period: float) -> Iterator[pb.Temperature]:
    """Generate ``n`` readings following a temperature profile."""
    start, slope = PROFILES[profile]
    rnd = random.Random(city)
    for i in range(n):
        time.sleep(period)
        yield pb.Temperature(city=city, celsius=round(start + slope * i + rnd.uniform(-0.4, 0.4), 1),
                             timestamp_ms=now_ms())


def main() -> None:
    """Open one client-streaming call and push all readings through it."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="127.0.0.1", help="ingest service host (e.g. the AWS public IP)")
    ap.add_argument("--timeout", type=float, default=60, help="seconds to wait for the whole stream")
    ap.add_argument("--n", type=int, default=12)
    ap.add_argument("--profile", choices=list(PROFILES), default="mild")
    ap.add_argument("--period", type=float, default=0.2)
    a = ap.parse_args()
    with grpc.insecure_channel(f"{a.host}:{a.port}") as ch:
        stub = pb_grpc.WeatherStub(ch)
        summary = stub.UploadReadings(readings(a.city, a.n, a.profile, a.period), timeout=a.timeout,
                                      wait_for_ready=True, metadata=(("sensor-id", f"sensor-{a.city}"),))
    log(f"sensor-{a.city}", f"done: server acknowledged {summary.received} readings (mean {summary.mean}°C)")


if __name__ == "__main__":
    main()
