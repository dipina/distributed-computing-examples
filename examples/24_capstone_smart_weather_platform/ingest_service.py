"""Component: INGEST service (gRPC server, the only writer of the commit log).

Sensors open a client-streaming ``UploadReadings`` call and push readings;
each reading is appended to the partitioned log (key = city).

    python ingest_service.py --data DIR --port 50051 [--host 0.0.0.0]

References:
    - gRPC Python basics: https://grpc.io/docs/languages/python/basics/
"""

from __future__ import annotations

import argparse
import sys
import threading
from concurrent import futures
from pathlib import Path
from typing import Iterator

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]
import grpc  # noqa: E402

from common.utils import log  # noqa: E402
from proto_stubs import pb, pb_grpc  # noqa: E402
from store import CommitLog  # noqa: E402


class IngestServicer(pb_grpc.WeatherServicer):
    """Implements only the ingestion RPC of the shared contract."""

    def __init__(self, commit_log: CommitLog) -> None:
        """Keep a handle on the log; a lock serialises concurrent streams."""
        self.log = commit_log
        self.lock = threading.Lock()

    # See: client-streaming RPCs, https://grpc.io/docs/languages/python/basics/#client-streaming-rpc
    def UploadReadings(self, request_iterator: Iterator[pb.Temperature], context: grpc.ServicerContext) -> pb.UploadSummary:  # noqa: N802
        """Client-streaming RPC: append every reading to the log."""
        n, total, sensor = 0, 0.0, dict(context.invocation_metadata()).get("sensor-id", "?")
        for t in request_iterator:
            with self.lock:
                p, off = self.log.append(t.city, {"celsius": t.celsius, "ts_ms": t.timestamp_ms, "sensor": sensor})
            n, total = n + 1, total + t.celsius
        log("ingest", f"stream from {sensor} closed: {n} readings appended to the log")
        return pb.UploadSummary(received=n, mean=round(total / n, 2) if n else 0.0)


def main() -> None:
    """Serve forever."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="127.0.0.1", help="bind address (0.0.0.0 inside a container)")
    a = ap.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_WeatherServicer_to_server(IngestServicer(CommitLog(a.data)), server)
    server.add_insecure_port(f"{a.host}:{a.port}")
    server.start()
    log("ingest", f"gRPC ingest service listening on {a.host}:{a.port}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
