"""The same weather MCP server, written with the official Python SDK (FastMCP).

Compare with ``serve_stdio`` in demo.py: the SDK derives the JSON Schemas from
type hints and the descriptions from docstrings, and handles the protocol.

    pip install mcp          # works with mcp 1.x (FastMCP) and mcp 2.x (MCPServer)
    python examples/23_ai_agent_protocols_mcp/fastmcp_server.py   # speaks MCP on stdio

It can also be plugged into any MCP host (e.g. Claude Desktop / Claude Code) as a
local stdio server.

References:
    - MCP Python SDK: https://py.sdk.modelcontextprotocol.io/
    - v2 migration (FastMCP renamed to MCPServer): https://py.sdk.modelcontextprotocol.io/v2/migration/#fastmcp-renamed-to-mcpserver
    - MCP Inspector: https://modelcontextprotocol.io/docs/tools/inspector
"""

# NOTE: no "from __future__ import annotations" here on purpose. Several mcp 1.x releases inspect the
# tool signatures at runtime (issubclass(param.annotation, Context)) and crash when annotations are
# strings. Real annotations (Python >= 3.10 syntax) work with every mcp version, 1.x and 2.x.

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
try:  # mcp >= 2.0 renamed FastMCP to MCPServer
    from mcp.server.mcpserver import MCPServer as FastMCP  # noqa: E402
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP  # noqa: E402

from common.domain import WeatherService  # noqa: E402

# See: https://py.sdk.modelcontextprotocol.io/
mcp = FastMCP("weather-mcp (FastMCP)")
svc = WeatherService()


@mcp.tool()
def get_temperature(city: str) -> dict[str, Any]:
    """Get the latest temperature in Celsius for a city."""
    return {"city": city, "celsius": svc.get_temperature(city)}


@mcp.tool()
def report_reading(city: str, celsius: float) -> dict[str, Any]:
    """Store a new temperature reading (Celsius) for a city."""
    return svc.report(city, celsius)


@mcp.tool()
def city_statistics(city: str) -> dict[str, Any]:
    """Get statistics (mean, min, max, count) of the temperatures of a city."""
    return svc.summary(city)


@mcp.resource("weather://stations", mime_type="application/json")
def stations() -> str:
    """List of cities with a weather station."""
    return json.dumps(svc.cities())


if __name__ == "__main__":
    mcp.run()  # stdio transport by default
