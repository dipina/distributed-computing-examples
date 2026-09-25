"""Component: MCP server that lets an AI agent use the platform (tools call the GraphQL API).

Stdio transport, JSON-RPC 2.0 (see example 23). Standard library only.

    python mcp_weather_server.py --api http://127.0.0.1:8080

References:
    - MCP specification 2025-11-25: https://modelcontextprotocol.io/specification/2025-11-25
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from typing import Any, Callable

PROTOCOL_VERSION = "2025-11-25"


def graphql(api: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """POST a GraphQL query to the platform API."""
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(f"{api}/graphql", body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())["data"]


# See: https://modelcontextprotocol.io/specification/2025-11-25/server/tools
def build_tools(api: str) -> tuple[list[dict[str, Any]], dict[str, Callable[[dict[str, Any]], Any]]]:
    """Tool descriptors (for tools/list) and their implementations."""
    city_schema = {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}
    tools = [
        {"name": "list_stations", "description": "List all weather stations with mean and max temperature.",
         "inputSchema": {"type": "object", "properties": {}}},
        {"name": "station_report", "description": "Detailed statistics and recent alerts for one city.",
         "inputSchema": city_schema},
        {"name": "active_alerts", "description": "Latest heat alerts (ORANGE >= 35°C, RED >= 40°C) in the network.",
         "inputSchema": {"type": "object", "properties": {"level": {"type": "string", "enum": ["ORANGE", "RED"]}}}},
    ]
    impl: dict[str, Callable[[dict[str, Any]], Any]] = {
        "list_stations": lambda a: graphql(api, "{ stations { city mean max } }")["stations"],
        "station_report": lambda a: graphql(
            api, "query($c: String!) { station(city: $c) { city count mean min max last alerts(limit: 3) { level celsius } } }",
            {"c": a["city"]})["station"],
        "active_alerts": lambda a: graphql(
            api, "query($l: String) { alerts(level: $l, limit: 50) { city level celsius } }", {"l": a.get("level")})["alerts"],
    }
    return tools, impl


def main() -> None:
    """Serve MCP over stdio."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True)
    tools, impl = build_tools(ap.parse_args().api)

    def reply(rid: Any, result: Any) -> None:
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}) + "\n")
        sys.stdout.flush()

    for line in sys.stdin:
        req = json.loads(line)
        rid, method, params = req.get("id"), req.get("method"), req.get("params", {})
        if rid is None:
            continue
        if method == "initialize":
            reply(rid, {"protocolVersion": PROTOCOL_VERSION, "capabilities": {"tools": {}},
                        "serverInfo": {"name": "smart-weather-platform", "version": "1.0"}})
        elif method == "tools/list":
            reply(rid, {"tools": tools})
        elif method == "tools/call":
            try:
                data = impl[params["name"]](params.get("arguments", {}))
                reply(rid, {"content": [{"type": "text", "text": json.dumps(data)}], "isError": False})
            except Exception as exc:  # noqa: BLE001
                reply(rid, {"content": [{"type": "text", "text": repr(exc)}], "isError": True})
        else:
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": rid,
                                         "error": {"code": -32601, "message": "Method not found"}}) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
