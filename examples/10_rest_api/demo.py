"""10 · RESTful API with FastAPI + Pydantic + OpenAPI (slides 15-25, 41-58).

REST models the system as **resources** identified by URIs, manipulated with
the uniform HTTP interface (GET, POST, PUT, DELETE...), using representations
(JSON), **stateless** requests, standard **status codes**, **cacheability**
(ETag / 304) and hypermedia links (HATEOAS).

Resources:
    /stations                      collection of weather stations
    /stations/{city}               one station
    /stations/{city}/readings      sub-collection of readings

Everything is served by a real uvicorn server and consumed with ``httpx``.
FastAPI generates the OpenAPI 3.1 contract automatically (see /docs).

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install fastapi uvicorn httpx     # or: pip install "fastapi[standard]"
    python examples/10_rest_api/demo.py --role server --port 8000
    curl -i http://127.0.0.1:8000/stations/bilbao
    curl -i -X POST -H 'Content-Type: application/json' -d '{"temperature": 21.5}' http://127.0.0.1:8000/stations/bilbao/readings
    open http://127.0.0.1:8000/docs    (Swagger UI)  |  http://127.0.0.1:8000/redoc

Tutorials & references:
    - FastAPI tutorial - user guide
      https://fastapi.tiangolo.com/tutorial/
    - FastAPI: first steps
      https://fastapi.tiangolo.com/tutorial/first-steps/
    - The Ultimate FastAPI Tutorial (slides 50-58)
      https://christophergs.com/tutorials/ultimate-fastapi-tutorial-pt-1-hello-world/
    - Pydantic documentation
      https://docs.pydantic.dev/
    - Uvicorn (ASGI server)
      https://www.uvicorn.org/
    - HTTPX (HTTP client)
      https://www.python-httpx.org/
    - OpenAPI Specification
      https://spec.openapis.org/oas/latest.html
    - RFC 9110: HTTP Semantics (methods, status codes, ETag)
      https://www.rfc-editor.org/rfc/rfc9110
    - Fielding's dissertation, ch. 5 (REST)
      https://ics.uci.edu/~fielding/pubs/dissertation/rest_arch_style.htm

Run the demo:        python examples/10_rest_api/demo.py
Explore in browser:  python examples/10_rest_api/demo.py --role server --port 8000
                     then open http://127.0.0.1:8000/docs
Requires:            pip install fastapi uvicorn httpx
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import UvicornThread, banner, log, require, section, takeaway  # noqa: E402

fastapi = require("fastapi", "fastapi uvicorn")
httpx = require("httpx")
require("uvicorn")

from fastapi import FastAPI, Header, HTTPException, Query, Response, status  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from common.domain import INITIAL_READINGS, Station  # noqa: E402


# ----------------------------------------------------------------- schemas
# See: https://fastapi.tiangolo.com/tutorial/body/ and https://docs.pydantic.dev/latest/concepts/models/
class ReadingIn(BaseModel):
    """Body of POST /stations/{city}/readings."""

    temperature: float = Field(..., ge=-90, le=60, description="Temperature in °C")


class StationIn(BaseModel):
    """Body of PUT /stations/{city}."""

    readings: list[float] = Field(default_factory=list)


class StationOut(BaseModel):
    """Representation of a station (with hypermedia links)."""

    city: str
    count: int
    last: float | None
    mean: float | None
    links: dict[str, str]


# ----------------------------------------------------------------- app
def create_app() -> FastAPI:
    """Build the FastAPI application (a fresh in-memory 'database' each time)."""
    # See: https://fastapi.tiangolo.com/tutorial/first-steps/
    app = FastAPI(title="Weather Stations API", version="1.0.0",
                  description="REST example for the Cloud Computing course (Unit 0)")
    db: dict[str, Station] = {c: Station(c, list(r)) for c, r in INITIAL_READINGS.items()}

    def represent(st: Station) -> StationOut:
        s = st.summary()
        return StationOut(city=st.city, count=s["count"], last=s["last"], mean=s["mean"], links={
            "self": f"/stations/{st.city}", "readings": f"/stations/{st.city}/readings", "collection": "/stations"})

    def get_or_404(city: str) -> Station:
        if city not in db:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"station '{city}' not found")
        return db[city]

    @app.get("/stations", response_model=list[StationOut], tags=["stations"])
    def list_stations(min_mean: float | None = Query(None, description="filter: mean >= min_mean"),
                      limit: int = Query(10, ge=1, le=100)) -> list[StationOut]:
        """List stations (collection resource) with filtering and pagination."""
        items = [represent(s) for s in db.values()]
        if min_mean is not None:
            items = [i for i in items if (i.mean or -999) >= min_mean]
        return items[:limit]

    @app.get("/stations/{city}", response_model=StationOut, tags=["stations"])
    def get_station(city: str, response: Response,
                    if_none_match: str | None = Header(None)) -> StationOut | Response:
        """Get one station. Supports conditional GET with ETag (cacheability)."""
        rep = represent(get_or_404(city))
        etag = '"' + hashlib.sha1(rep.model_dump_json().encode()).hexdigest()[:16] + '"'
        # See: RFC 9110 conditional requests, https://www.rfc-editor.org/rfc/rfc9110#name-if-none-match
        if if_none_match == etag:
            return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers={"ETag": etag})
        response.headers["ETag"] = etag
        response.headers["Cache-Control"] = "max-age=30"
        return rep

    @app.put("/stations/{city}", response_model=StationOut, tags=["stations"])
    def put_station(city: str, body: StationIn, response: Response) -> StationOut:
        """Create or fully replace a station (idempotent)."""
        response.status_code = status.HTTP_200_OK if city in db else status.HTTP_201_CREATED
        db[city] = Station(city, body.readings)
        return represent(db[city])

    @app.delete("/stations/{city}", status_code=status.HTTP_204_NO_CONTENT, tags=["stations"])
    def delete_station(city: str) -> Response:
        """Delete a station (idempotent in effect)."""
        get_or_404(city)
        del db[city]
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/stations/{city}/readings", tags=["readings"])
    def list_readings(city: str) -> list[float]:
        """Sub-collection of readings."""
        return get_or_404(city).readings

    @app.post("/stations/{city}/readings", status_code=status.HTTP_201_CREATED,
              response_model=StationOut, tags=["readings"])
    def add_reading(city: str, body: ReadingIn, response: Response) -> StationOut:
        """Append a reading (NOT idempotent: repeating it adds another one)."""
        st = get_or_404(city)
        st.readings.append(body.temperature)
        response.headers["Location"] = f"/stations/{city}/readings/{len(st.readings) - 1}"
        return represent(st)

    return app


def show(r: "httpx.Response") -> None:
    """Log a request/response pair in a compact way."""
    body = r.text if len(r.text) < 170 else r.text[:167] + "..."
    log("http-client", f"{r.request.method:6} {r.request.url.path}{'?' + r.request.url.query.decode() if r.request.url.query else ''}"
                       f"  -> {r.status_code} {body}")


def run_demo(base: str) -> None:
    """Exercise the API as an HTTP client would."""
    with httpx.Client(base_url=base) as c:
        section("GET collection, filters (query params), a single resource")
        show(c.get("/stations", params={"min_mean": 20}))
        r = c.get("/stations/bilbao")
        show(r)
        log("http-client", f"follow hypermedia link 'readings' -> {c.get(r.json()['links']['readings']).json()}")

        section("POST (create in sub-collection, 201 + Location) vs PUT (idempotent create/replace)")
        r = c.post("/stations/bilbao/readings", json={"temperature": 23.4})
        show(r)
        log("http-client", f"Location header: {r.headers['location']}")
        show(c.put("/stations/lisbon", json={"readings": [21.0, 22.5]}))
        show(c.put("/stations/lisbon", json={"readings": [21.0, 22.5]}))  # same result -> idempotent

        section("Errors: 404 unknown resource, 422 validation by Pydantic")
        show(c.get("/stations/atlantis"))
        show(c.post("/stations/bilbao/readings", json={"temperature": 999}))

        section("Cacheability: ETag + conditional GET -> 304 Not Modified (no body transferred)")
        r1 = c.get("/stations/oslo")
        log("http-client", f"ETag={r1.headers['etag']} Cache-Control={r1.headers['cache-control']}")
        show(c.get("/stations/oslo", headers={"If-None-Match": r1.headers["etag"]}))

        section("DELETE, then the resource is gone")
        show(c.delete("/stations/lisbon"))
        show(c.get("/stations/lisbon"))

        section("The OpenAPI contract is generated from the code")
        spec = c.get("/openapi.json").json()
        log("openapi", f"version {spec['openapi']}, paths: {list(spec['paths'])}")
        out = Path(__file__).with_name("openapi.json")
        out.write_text(json.dumps(spec, indent=2))
        log("openapi", f"saved to {out.relative_to(Path.cwd()) if out.is_relative_to(Path.cwd()) else out}")


def main() -> None:
    """Entry point supporting ``--role demo|server``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["demo", "server"], default="demo")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    banner("10 · RESTful API: FastAPI + Pydantic + OpenAPI", "15-25, 41-58")
    if args.role == "server":
        import uvicorn
        log("server", f"docs at http://127.0.0.1:{args.port}/docs")
        uvicorn.run(create_app(), host="127.0.0.1", port=args.port)
        return
    with UvicornThread(create_app()) as srv:
        log("server", f"uvicorn serving on {srv.url}")
        run_demo(srv.url)
    takeaway(
        "Nouns, not verbs: resources + uniform interface (GET/POST/PUT/DELETE) instead of custom procedures.",
        "Status codes carry semantics: 200, 201+Location, 204, 304, 404, 422...",
        "Statelessness + cacheability (ETag) are what make REST scale behind proxies and CDNs.",
        "PUT/DELETE are idempotent, POST is not - crucial when clients retry after timeouts.",
        "With FastAPI the OpenAPI contract comes for free from type hints (see /docs).",
    )


if __name__ == "__main__":
    main()
