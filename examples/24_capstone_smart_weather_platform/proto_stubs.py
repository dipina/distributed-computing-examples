"""Compile the gRPC contract of example 12 (``12_grpc/protos/weather.proto``) and expose the stubs.

The capstone REUSES the same contract: ``UploadReadings`` (client streaming) is the ingestion API.

References:
    - gRPC Python: generate gRPC code: https://grpc.io/docs/languages/python/quickstart/#generate-grpc-code
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROTO_DIR = HERE.parent / "12_grpc" / "protos"
OUT = HERE / "generated"


def _compile() -> None:
    from grpc_tools import protoc

    OUT.mkdir(exist_ok=True)
    if not (OUT / "weather_pb2.py").exists():
        rc = protoc.main(["protoc", f"-I{PROTO_DIR}", f"--python_out={OUT}", f"--grpc_python_out={OUT}",
                          str(PROTO_DIR / "weather.proto")])
        if rc != 0:
            raise RuntimeError("protoc failed")
    sys.path.insert(0, str(OUT))


_compile()
import weather_pb2 as pb  # noqa: E402
import weather_pb2_grpc as pb_grpc  # noqa: E402

__all__ = ["pb", "pb_grpc"]
