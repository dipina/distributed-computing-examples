"""Component: public API (CQRS query side) - GraphQL + Server-Sent Events + REST health.

* ``POST /graphql``           GraphQL queries over the read model (stations, alerts, pipeline lag)
* ``GET  /alerts/stream``     SSE feed of new alerts for dashboards (``?after=<id>``)
* ``GET  /health``            REST liveness/readiness probe used by the supervisor

    python api_service.py --data DIR --port 8080 [--host 0.0.0.0]

References:
    - Strawberry + FastAPI: https://strawberry.rocks/docs/integrations/fastapi
    - MDN: server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import AsyncIterator, Optional

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]
import strawberry  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402
from strawberry.fastapi import GraphQLRouter  # noqa: E402

from common.utils import log  # noqa: E402
from store import CommitLog, ReadModel  # noqa: E402

RM: ReadModel
LOG: CommitLog


@strawberry.type
class Alert:
    """A heat alert raised by the analytics workers."""

    id: int
    city: str
    celsius: float
    level: str
    ts_ms: float


@strawberry.type
class Station:
    """Materialised statistics of one city."""

    city: str
    count: int
    mean: float
    min: float
    max: float
    last: float

    @strawberry.field
    def alerts(self, limit: int = 5) -> list[Alert]:
        """Alerts of this station (graph edge)."""
        return [Alert(**a) for a in RM.alerts(city=self.city, limit=limit)]


@strawberry.type
class PartitionLag:
    """Consumer-group progress on one log partition."""

    partition: int
    end_offset: int
    committed: int
    lag: int


@strawberry.type
class Query:
    """Read-only API: commands enter through gRPC ingestion, queries through GraphQL (CQRS)."""

    @strawberry.field
    def stations(self, min_mean: Optional[float] = None) -> list[Station]:
        """All stations, optionally only those with mean >= ``min_mean``."""
        return [Station(**s) for s in RM.stations() if min_mean is None or s["mean"] >= min_mean]

    @strawberry.field
    def station(self, city: str) -> Optional[Station]:
        """One station."""
        s = RM.station(city)
        return Station(**s) if s else None

    @strawberry.field
    def alerts(self, level: Optional[str] = None, limit: int = 20) -> list[Alert]:
        """Latest alerts."""
        return [Alert(**a) for a in RM.alerts(level=level, limit=limit)]

    @strawberry.field
    def pipeline(self) -> list[PartitionLag]:
        """Consumer lag per partition (is the read model up to date?)."""
        return [PartitionLag(**p) for p in RM.pipeline(LOG)]


def create_app(data: Path) -> FastAPI:
    """Build the FastAPI app."""
    global RM, LOG
    RM, LOG = ReadModel(data), CommitLog(data)
    app = FastAPI(title="Smart Weather Platform API")
    # See: https://strawberry.rocks/docs/integrations/fastapi
    app.include_router(GraphQLRouter(strawberry.Schema(query=Query)), prefix="/graphql")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "up"}

    @app.get("/alerts/stream")
    async def stream(after: int = 0) -> StreamingResponse:
        """SSE: push alerts as soon as they appear in the read model."""
        async def gen() -> AsyncIterator[str]:
            last = after
            while True:
                for a in RM.alerts(after_id=last):
                    last = a["id"]
                    yield f"id: {a['id']}\nevent: alert\ndata: {json.dumps(a)}\n\n"
                await asyncio.sleep(0.2)
        return StreamingResponse(gen(), media_type="text/event-stream")

    return app


def main() -> None:
    """Serve with uvicorn."""
    import uvicorn

    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--host", default="127.0.0.1", help="bind address (0.0.0.0 inside a container)")
    a = ap.parse_args()
    log("api", f"GraphQL on http://{a.host}:{a.port}/graphql, SSE on /alerts/stream")
    uvicorn.run(create_app(a.data), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
