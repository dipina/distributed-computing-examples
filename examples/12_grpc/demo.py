"""12 · gRPC: contract-first RPC over HTTP/2 with Protocol Buffers (slide 20).

gRPC is the modern incarnation of RPC (after XML-RPC and SOAP):

* **Contract first**: the API is defined in ``protos/weather.proto`` (IDL);
  client and server stubs are *generated* for many languages.
* **Protocol Buffers**: compact, typed, binary serialisation.
* **HTTP/2**: multiplexing and **streaming** in both directions.
* Built-in **deadlines**, **status codes** and metadata.

The demo compiles the .proto at start-up and shows the four RPC kinds:
unary, server-streaming, client-streaming and bidirectional streaming.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    pip install grpcio grpcio-tools
    python -m grpc_tools.protoc -Iexamples/12_grpc/protos --python_out=. --grpc_python_out=. examples/12_grpc/protos/weather.proto      # what demo.py does automatically
    grpcurl (CLI client, like curl for gRPC): https://github.com/fullstorydev/grpcurl  (brew install grpcurl)
    grpcurl -plaintext -proto examples/12_grpc/protos/weather.proto -d '{"city":"bilbao"}' localhost:<port> weather.Weather/GetTemperature

Tutorials & references:
    - gRPC Python quick start
      https://grpc.io/docs/languages/python/quickstart/
    - gRPC Python basics tutorial (all 4 streaming kinds)
      https://grpc.io/docs/languages/python/basics/
    - gRPC generated-code reference (Python)
      https://grpc.io/docs/languages/python/generated-code/
    - Protocol Buffers: proto3 language guide
      https://protobuf.dev/programming-guides/proto3/
    - gRPC status codes
      https://grpc.io/docs/guides/status-codes/
    - gRPC deadlines
      https://grpc.io/docs/guides/deadlines/

Run:       python examples/12_grpc/demo.py
Requires:  pip install grpcio grpcio-tools
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from concurrent import futures
from pathlib import Path
from typing import Iterator

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, free_port, log, require, section, takeaway  # noqa: E402

grpc = require("grpc", "grpcio grpcio-tools")
require("grpc_tools", "grpcio-tools")


def compile_proto() -> None:
    """Generate ``weather_pb2.py`` and ``weather_pb2_grpc.py`` into ./generated."""
    from grpc_tools import protoc

    out = HERE / "generated"
    out.mkdir(exist_ok=True)
    # See: https://grpc.io/docs/languages/python/quickstart/#generate-grpc-code
    rc = protoc.main(["protoc", f"-I{HERE / 'protos'}", f"--python_out={out}",
                      f"--grpc_python_out={out}", str(HERE / "protos" / "weather.proto")])
    if rc != 0:
        raise RuntimeError("protoc failed")
    sys.path.insert(0, str(out))


compile_proto()
import weather_pb2 as pb  # noqa: E402
import weather_pb2_grpc as pb_grpc  # noqa: E402

SERVICE = WeatherService()


def now_ms() -> int:
    """Current time in milliseconds."""
    return int(time.time() * 1000)


class WeatherServicer(pb_grpc.WeatherServicer):
    """Server-side implementation of the generated interface."""

    def GetTemperature(self, request: pb.CityRequest, context: grpc.ServicerContext) -> pb.Temperature:  # noqa: N802
        """Unary RPC."""
        if request.city == "slowtown":
            time.sleep(1)  # to trigger the client's deadline
        try:
            t = SERVICE.get_temperature(request.city)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown city {request.city!r}")
        return pb.Temperature(city=request.city, celsius=t, timestamp_ms=now_ms())

    def WatchCity(self, request: pb.WatchRequest, context: grpc.ServicerContext) -> Iterator[pb.Temperature]:  # noqa: N802
        """Server-streaming RPC: push samples as they are produced."""
        base = SERVICE.get_temperature(request.city)
        for i in range(request.samples):
            time.sleep(0.1)
            yield pb.Temperature(city=request.city, celsius=round(base + 0.3 * i, 2), timestamp_ms=now_ms())

    def UploadReadings(self, request_iterator: Iterator[pb.Temperature], context: grpc.ServicerContext) -> pb.UploadSummary:  # noqa: N802
        """Client-streaming RPC: consume a stream, answer once."""
        values: list[float] = []
        for t in request_iterator:
            SERVICE.report(t.city, t.celsius)
            values.append(t.celsius)
        return pb.UploadSummary(received=len(values), mean=round(statistics.fmean(values), 2))

    def LiveAlerts(self, request_iterator: Iterator[pb.Temperature], context: grpc.ServicerContext) -> Iterator[pb.Alert]:  # noqa: N802
        """Bidirectional streaming: answer each reading as it arrives."""
        for t in request_iterator:
            level = "RED" if t.celsius >= 40 else "YELLOW" if t.celsius >= 35 else "OK"
            yield pb.Alert(city=t.city, level=level, message=f"{t.celsius}°C")


def main() -> None:
    """Start the server, run the four RPC kinds from a client."""
    banner("12 · gRPC: Protocol Buffers, HTTP/2 and streaming", "20")
    port = free_port()
    # See: https://grpc.io/docs/languages/python/basics/#starting-the-server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_WeatherServicer_to_server(WeatherServicer(), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    log("server", f"gRPC server on :{port} (stubs generated from protos/weather.proto)")

    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        stub = pb_grpc.WeatherStub(channel)

        section("1) Unary RPC + payload size: protobuf vs JSON")
        t = stub.GetTemperature(pb.CityRequest(city="bilbao"), timeout=2)
        log("client", f"GetTemperature(bilbao) -> {t.celsius}°C")
        as_json = json.dumps({"city": t.city, "celsius": t.celsius, "timestamp_ms": t.timestamp_ms})
        log("client", f"protobuf message = {len(t.SerializeToString())} bytes vs JSON = {len(as_json)} bytes")

        section("Status codes and deadlines")
        for city, timeout in (("atlantis", 2.0), ("slowtown", 0.3)):
            try:
                stub.GetTemperature(pb.CityRequest(city=city), timeout=timeout)
            except grpc.RpcError as e:
                log("client", f"GetTemperature({city}) -> {e.code().name}: {e.details()}")

        section("2) Server streaming: subscribe to a city and receive a stream")
        for sample in stub.WatchCity(pb.WatchRequest(city="madrid", samples=4)):
            log("client", f"stream item: {sample.city} {sample.celsius}°C")

        section("3) Client streaming: upload many readings, get one summary")
        def readings() -> Iterator[pb.Temperature]:
            for v in (18.0, 18.6, 19.1, 20.3):
                log("client", f"sending {v}")
                yield pb.Temperature(city="bilbao", celsius=v, timestamp_ms=now_ms())
        s = stub.UploadReadings(readings())
        log("client", f"server summary: received={s.received} mean={s.mean}")

        section("4) Bidirectional streaming: alerts come back while we are still sending")
        def live() -> Iterator[pb.Temperature]:
            for v in (31.0, 36.5, 41.2, 33.0):
                time.sleep(0.05)
                yield pb.Temperature(city="sevilla", celsius=v, timestamp_ms=now_ms())
        for alert in stub.LiveAlerts(live()):
            log("client", f"alert: {alert.city} {alert.level:<6} {alert.message}")

    server.stop(grace=None)
    takeaway(
        "Contract-first: the .proto is the single source of truth; stubs are generated (polyglot).",
        "Binary protobuf is smaller and faster to parse than JSON; HTTP/2 enables 4 streaming modes.",
        "Deadlines and rich status codes are first-class - essential between microservices.",
        "Typical split: REST/GraphQL at the edge (browsers), gRPC inside the cluster (service-to-service).",
    )


if __name__ == "__main__":
    main()
