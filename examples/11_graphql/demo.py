"""11 · GraphQL (slide 26).

GraphQL exposes ONE endpoint and a strongly-typed **schema**. The client
sends a *query* describing exactly the shape of the data it wants; the server
resolves it field by field. This solves REST's typical problems:

* **over-fetching**  - REST returns the whole representation; GraphQL only the requested fields;
* **under-fetching / N+1 round trips** - nested data in ONE request;
* evolving clients without versioning the API, plus built-in **introspection**.

We use Strawberry (type-hint based, like FastAPI) mounted on FastAPI.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install strawberry-graphql fastapi uvicorn httpx
    python examples/11_graphql/demo.py --role server --port 8001
    open http://127.0.0.1:8001/graphql   (GraphiQL IDE in the browser)
    curl -s -X POST -H 'Content-Type: application/json' -d '{"query": "{ stations { city mean } }"}' http://127.0.0.1:8001/graphql

Tutorials & references:
    - Introduction to GraphQL (official tutorial)
      https://graphql.org/learn/
    - GraphQL specification
      https://spec.graphql.org/
    - Strawberry: getting started
      https://strawberry.rocks/docs
    - Strawberry + FastAPI integration
      https://strawberry.rocks/docs/integrations/fastapi
    - Graphene (alternative Python library)
      https://graphene-python.org/

Run:       python examples/11_graphql/demo.py
Explore:   python examples/11_graphql/demo.py --role server  -> http://127.0.0.1:8001/graphql (GraphiQL)
Requires:  pip install strawberry-graphql fastapi uvicorn httpx
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import UvicornThread, banner, log, require, section, takeaway  # noqa: E402

strawberry = require("strawberry", "strawberry-graphql")
require("fastapi")
httpx = require("httpx")

from fastapi import FastAPI  # noqa: E402
from strawberry.fastapi import GraphQLRouter  # noqa: E402

from common.domain import WeatherService  # noqa: E402

logging.getLogger("strawberry.execution").setLevel(logging.CRITICAL)  # keep demo output clean
SERVICE = WeatherService()
COUNTRY_OF = {"bilbao": "Spain", "madrid": "Spain", "barcelona": "Spain", "oslo": "Norway"}


# See: https://strawberry.rocks/docs/types/object-types
@strawberry.type
class Station:
    """A weather station."""

    city: str

    @strawberry.field
    def last(self) -> Optional[float]:
        """Latest reading."""
        return SERVICE.summary(self.city)["last"]

    @strawberry.field
    def mean(self) -> Optional[float]:
        """Average temperature."""
        return SERVICE.summary(self.city)["mean"]

    @strawberry.field
    def readings(self, last: int = 100) -> list[float]:
        """The last N readings (fields can take arguments)."""
        return SERVICE.station(self.city).readings[-last:]

    @strawberry.field
    def country(self) -> "Country":
        """Edge of the graph: station -> country."""
        return Country(name=COUNTRY_OF.get(self.city, "Unknown"))


@strawberry.type
class Country:
    """A country, linked to its stations (the 'graph' in GraphQL)."""

    name: str

    @strawberry.field
    def stations(self) -> list[Station]:
        """Edge of the graph: country -> stations."""
        return [Station(city=c) for c in SERVICE.cities() if COUNTRY_OF.get(c) == self.name]


@strawberry.type
class Query:
    """Read operations."""

    @strawberry.field
    def stations(self, min_mean: Optional[float] = None) -> list[Station]:
        """All stations, optionally filtered."""
        out = [Station(city=c) for c in SERVICE.cities()]
        return [s for s in out if min_mean is None or (s.mean() or -999) >= min_mean]

    @strawberry.field
    def station(self, city: str) -> Optional[Station]:
        """One station by city."""
        return Station(city=city) if city in SERVICE.cities() else None


@strawberry.type
class Mutation:
    """Write operations."""

    @strawberry.mutation
    def add_reading(self, city: str, temperature: float) -> Station:
        """Append a reading and return the updated station."""
        if not -90 <= temperature <= 60:
            raise ValueError("temperature out of range")
        SERVICE.report(city, temperature)
        return Station(city=city.lower())


schema = strawberry.Schema(query=Query, mutation=Mutation)


def create_app() -> FastAPI:
    """FastAPI app with the GraphQL router at /graphql."""
    app = FastAPI()
    # See: https://strawberry.rocks/docs/integrations/fastapi
    app.include_router(GraphQLRouter(schema), prefix="/graphql")
    return app


def gql(client: "httpx.Client", query: str, variables: dict | None = None) -> dict:
    """POST a GraphQL operation and log it."""
    r = client.post("/graphql", json={"query": query, "variables": variables or {}})
    compact = " ".join(query.split())
    log("gql-client", f"{compact[:95]}{'…' if len(compact) > 95 else ''}")
    log("gql-client", f"  -> HTTP {r.status_code}, {len(r.content)} bytes: {json.dumps(r.json())[:150]}")
    return r.json()


def run_demo(base: str) -> None:
    """Run a few queries showing GraphQL's strengths."""
    with httpx.Client(base_url=base) as c:
        section("Ask for exactly the fields you need (no over-fetching)")
        gql(c, "{ stations { city } }")
        gql(c, "{ stations(minMean: 20) { city mean } }")

        section("Nested data in ONE round trip (REST would need 1 + N requests)")
        gql(c, """{ station(city: "bilbao") { city last readings(last: 2)
                    country { name stations { city mean } } } }""")

        section("Mutations with variables")
        gql(c, "mutation Add($c: String!, $t: Float!) { addReading(city: $c, temperature: $t) { city last mean } }",
            {"c": "oslo", "t": 12.5})

        section("Errors come in an 'errors' array (partial results are possible)")
        gql(c, 'mutation { addReading(city: "oslo", temperature: 999) { city } }')
        gql(c, "{ stations { city humidity } }")

        section("Introspection: the schema is self-describing")
        res = gql(c, "{ __schema { queryType { fields { name } } mutationType { fields { name } } } }")
        log("schema", f"queries={[f['name'] for f in res['data']['__schema']['queryType']['fields']]}, "
                      f"mutations={[f['name'] for f in res['data']['__schema']['mutationType']['fields']]}")
        print("\nSDL generated from the Python types:\n" + schema.as_str()[:600] + "\n...")


def main() -> None:
    """Entry point supporting ``--role demo|server``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["demo", "server"], default="demo")
    ap.add_argument("--port", type=int, default=8001)
    args = ap.parse_args()
    banner("11 · GraphQL: one endpoint, client-shaped queries", "26")
    if args.role == "server":
        import uvicorn
        log("server", f"GraphiQL at http://127.0.0.1:{args.port}/graphql")
        uvicorn.run(create_app(), host="127.0.0.1", port=args.port)
        return
    with UvicornThread(create_app()) as srv:
        run_demo(srv.url)
    takeaway(
        "One endpoint + typed schema; the CLIENT decides the shape of the response.",
        "Avoids over-fetching and N+1 round trips - great for mobile and aggregating UIs.",
        "Trade-offs: HTTP caching is harder (POST to one URL), and costly queries need limits.",
        "Often used as a BFF/gateway layer in front of REST/gRPC microservices (see 15).",
    )


if __name__ == "__main__":
    main()
