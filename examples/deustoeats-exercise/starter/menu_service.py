"""menu-service: internal gRPC service that validates and prices orders (Part A - TO DO).

    python menu_service.py            # listens on MENU_ADDR (default 127.0.0.1:50061)

The contract is menu.proto (do not change it). common.compile_proto() generates menu_pb2*.py for you.
"""

from __future__ import annotations

import time
from concurrent import futures
from typing import Iterator

import grpc

from common import MAX_PREP_MS, MENU, MENU_ADDR, compile_proto, log  # noqa: F401  (you will need them)

compile_proto()
import menu_pb2 as pb  # noqa: E402
import menu_pb2_grpc as pb_grpc  # noqa: E402


class MenuServicer(pb_grpc.MenuServicer):
    """Implementation of the generated ``Menu`` service interface."""

    def Quote(self, request: pb.QuoteRequest, context: grpc.ServicerContext) -> pb.QuoteReply:  # noqa: N802
        """Validate every line and return the total price and the preparation time.

        TODO A1: implement the rules of the README:
          * empty order or qty <= 0  -> context.abort(grpc.StatusCode.INVALID_ARGUMENT, "...")
          * unknown sku               -> context.abort(grpc.StatusCode.NOT_FOUND, "...")
          * qty > stock               -> context.abort(grpc.StatusCode.FAILED_PRECONDITION, "...")
          * total_cents = sum(price_cents * qty);  prep_ms = min(MAX_PREP_MS, sum(prep_ms * qty))
        Hint: the menu data is the MENU dict in common.py.
        """
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "TODO A1: Quote")

    def ListItems(self, request: pb.Empty, context: grpc.ServicerContext) -> Iterator[pb.Item]:  # noqa: N802
        """Server-streaming RPC: one message per menu item.

        TODO A2: ``yield`` one pb.Item per entry of MENU.
        """
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "TODO A2: ListItems")


def main() -> None:
    """Start the gRPC server and block (GIVEN)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_MenuServicer_to_server(MenuServicer(), server)
    server.add_insecure_port(MENU_ADDR)
    server.start()
    log("menu", f"gRPC menu-service listening on {MENU_ADDR}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
