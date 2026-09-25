"""23 · AI-agent protocols: the Model Context Protocol (MCP)  [NEW].

The newest distributed-computing paradigm connects **AI agents (LLM apps)** to
**tools and data** living in other processes/machines. MCP (Anthropic, 2024;
now an open standard adopted across the industry) is essentially:

* **JSON-RPC 2.0** messages (the RPC of example 05!)...
* ...over a **transport**: stdio (local subprocess) or Streamable HTTP (+SSE, example 13),
* with a **capability handshake** (``initialize``) and **dynamic discovery**:
  ``tools/list`` returns each tool's name, description and JSON-Schema input,
  so an agent can decide at runtime which tools to call (``tools/call``);
  ``resources/list`` / ``resources/read`` expose data.

Echoes of older paradigms: a **network service** with a directory (06), a
**mobile agent** planning its itinerary over hosts (08), and **RPC** stubs (05) -
except the "client stub" is now an LLM reading tool descriptions.

This demo:
1. runs a tiny hand-written MCP server (stdlib only) as a **subprocess over stdio**;
2. a client performs the handshake, discovers tools/resources and calls them;
3. a toy rule-based "agent" (no LLM needed) picks tools from their descriptions;
4. if the official SDK is installed (``pip install mcp``), the SAME client talks to an
   equivalent server written with ``FastMCP`` (``fastmcp_server.py``) -> interoperability.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing for part A: Python standard library only)
    pip install "mcp[cli]"     # official SDK, used by fastmcp_server.py (part B)
    npx @modelcontextprotocol/inspector python examples/23_ai_agent_protocols_mcp/fastmcp_server.py
                                # MCP Inspector: browse/call the tools in a web UI (needs Node.js)

Tutorials & references:
    - Model Context Protocol: specification 2025-11-25
      https://modelcontextprotocol.io/specification/2025-11-25
    - MCP: lifecycle (initialize handshake)
      https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
    - MCP: tools
      https://modelcontextprotocol.io/specification/2025-11-25/server/tools
    - MCP Python SDK documentation
      https://py.sdk.modelcontextprotocol.io/
    - MCP Inspector
      https://modelcontextprotocol.io/docs/tools/inspector
    - Agent2Agent (A2A) protocol
      https://a2a-protocol.org/latest/
    - JSON-RPC 2.0 specification
      https://www.jsonrpc.org/specification

Run:  python examples/23_ai_agent_protocols_mcp/demo.py
"""

from __future__ import annotations

import argparse
import importlib.util
import itertools
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, log, section, takeaway  # noqa: E402

PROTOCOL_VERSION = "2025-11-25"


# ====================================================================== the MCP server (stdio)
TOOLS: list[dict[str, Any]] = [
    {"name": "get_temperature",
     "description": "Get the latest temperature in Celsius for a city.",
     "inputSchema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}},
    {"name": "report_reading",
     "description": "Store a new temperature reading (Celsius) for a city.",
     "inputSchema": {"type": "object", "properties": {"city": {"type": "string"}, "celsius": {"type": "number"}},
                     "required": ["city", "celsius"]}},
    {"name": "city_statistics",
     "description": "Get statistics (mean, min, max, count) of the temperatures of a city.",
     "inputSchema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}},
]


def serve_stdio() -> None:
    """Minimal MCP server: newline-delimited JSON-RPC 2.0 on stdin/stdout. Logs go to stderr."""
    svc = WeatherService()
    impl = {"get_temperature": lambda a: {"city": a["city"], "celsius": svc.get_temperature(a["city"])},
            "report_reading": lambda a: svc.report(a["city"], a["celsius"]),
            "city_statistics": lambda a: svc.summary(a["city"])}

    def reply(rid: Any, result: Any = None, error: dict[str, Any] | None = None) -> None:
        msg = {"jsonrpc": "2.0", "id": rid, **({"error": error} if error else {"result": result})}
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()

    for line in sys.stdin:
        req = json.loads(line)
        method, rid, params = req.get("method"), req.get("id"), req.get("params", {})
        if rid is None:  # notification (e.g. notifications/initialized): no reply
            continue
        if method == "initialize":
            reply(rid, {"protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {"tools": {}, "resources": {}},
                        "serverInfo": {"name": "weather-mcp (hand-written)", "version": "1.0"}})
        elif method == "tools/list":
            reply(rid, {"tools": TOOLS})
        elif method == "tools/call":
            try:
                data = impl[params["name"]](params.get("arguments", {}))
                reply(rid, {"content": [{"type": "text", "text": json.dumps(data)}],
                            "structuredContent": data, "isError": False})
            except KeyError as exc:  # tool errors are reported IN the result, for the model to see
                reply(rid, {"content": [{"type": "text", "text": f"unknown city or tool: {exc}"}], "isError": True})
        elif method == "resources/list":
            reply(rid, {"resources": [{"uri": "weather://stations", "name": "stations",
                                       "description": "List of cities with a weather station",
                                       "mimeType": "application/json"}]})
        elif method == "resources/read":
            reply(rid, {"contents": [{"uri": params["uri"], "mimeType": "application/json",
                                      "text": json.dumps(svc.cities())}]})
        elif method == "ping":
            reply(rid, {})
        else:
            reply(rid, error={"code": -32601, "message": f"Method not found: {method}"})


# ====================================================================== the MCP client (host side)
class MCPClient:
    """Spawns an MCP server subprocess and talks JSON-RPC over its stdio."""

    def __init__(self, command: list[str], label: str) -> None:
        """Start the server process."""
        self.label = label
        self.stderr = tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace")
        self.proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                                     text=True, encoding="utf-8", bufsize=1)
        self.ids = itertools.count(1)

    def _send(self, msg: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send a request and wait for the response with the same id."""
        rid = next(self.ids)
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
        assert self.proc.stdout is not None
        while True:
            line = self.proc.stdout.readline()
            if not line:  # EOF: the server process died -> show WHY instead of a JSONDecodeError
                self.proc.wait(timeout=5)
                self.stderr.seek(0)
                tail = "\n".join(self.stderr.read().strip().splitlines()[-6:])
                raise RuntimeError(f"MCP server exited (code {self.proc.returncode}):\n{tail}")
            if not line.strip():
                continue
            msg = json.loads(line)
            if msg.get("id") == rid:
                if "error" in msg:
                    raise RuntimeError(msg["error"])
                return msg["result"]

    def notify(self, method: str) -> None:
        """Send a notification (no id, no response)."""
        self._send({"jsonrpc": "2.0", "method": method})

    # See: https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
    def initialize(self) -> dict[str, Any]:
        """Capability negotiation handshake."""
        res = self.request("initialize", {"protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                                          "clientInfo": {"name": "unit0-demo-host", "version": "1.0"}})
        self.notify("notifications/initialized")
        return res

    def close(self) -> None:
        """Terminate the server."""
        self.proc.terminate()
        self.proc.wait()
        self.stderr.close()


def compact(text: str) -> str:
    """Re-serialise JSON text on one line (servers may pretty-print)."""
    try:
        return json.dumps(json.loads(text))
    except ValueError:
        return text


# ====================================================================== a toy agent
def toy_agent(client: MCPClient, goal: str, tools: list[dict[str, Any]]) -> None:
    """Pick tools by matching the goal against tool DESCRIPTIONS (what an LLM does, but dumber)."""
    log("agent", f"goal: {goal!r}")
    cities = json.loads(client.request("resources/read", {"uri": "weather://stations"})["contents"][0]["text"])
    city = next((c for c in cities if c in goal.lower()), cities[0])
    stop = {"a", "the", "is", "in", "of", "for", "what", "how", "it", "get", "please", "now", "right"}
    synonyms = {"average": "mean", "hot": "temperature", "stats": "statistics"}

    def keywords(text: str) -> set[str]:
        words = (synonyms.get(w, w).rstrip("s") for w in re.findall(r"[a-z]+", text.lower()))
        return {w for w in words if w not in stop}

    wanted = keywords(goal)
    best = max(tools, key=lambda t: len(wanted & keywords(t["description"])))
    log("agent", f"plan: city={city!r} (from resource), tool={best['name']!r} (best description match)")
    res = client.request("tools/call", {"name": best["name"], "arguments": {"city": city}})
    log("agent", f"observation: {compact(res['content'][0]['text'])}")


def session(client: MCPClient) -> None:
    """Handshake, discovery, calls and a toy agent against one server."""
    init = client.initialize()
    log(client.label, f"initialize -> server={init['serverInfo']['name']!r}, protocol={init['protocolVersion']}, "
                      f"capabilities={sorted(init['capabilities'])}")
    tools = client.request("tools/list")["tools"]
    for t in tools:
        log(client.label, f"tool {t['name']:<16} {t.get('description', '').strip()[:60]!r} "
                          f"args={list(t['inputSchema'].get('properties', {}))}")
    r = client.request("tools/call", {"name": "report_reading", "arguments": {"city": "bilbao", "celsius": 23.5}})
    log(client.label, f"tools/call report_reading -> {compact(r['content'][0]['text'])}")
    r = client.request("tools/call", {"name": "get_temperature", "arguments": {"city": "atlantis"}})
    log(client.label, f"tools/call with bad input -> isError={r.get('isError')} (errors go back to the model)")
    toy_agent(client, "What is the average temperature in Oslo?", tools)
    toy_agent(client, "How hot is it right now in Madrid? latest temperature please", tools)


def main() -> None:
    """Run the client against the hand-written server, then against FastMCP if available."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="run as MCP stdio server")
    if ap.parse_args().serve:
        serve_stdio()
        return

    banner("23 · AI-agent protocols: Model Context Protocol (JSON-RPC over stdio)", added=True)
    section("A) Hand-written MCP server (stdlib only) running as a subprocess")
    c = MCPClient([sys.executable, __file__, "--serve"], "mcp-client")
    session(c)
    c.close()

    section("B) Same client vs. a server built with the official SDK (FastMCP) - interoperability")
    if importlib.util.find_spec("mcp") is None:
        log("mcp-client", "official SDK not installed -> skipped (pip install mcp)")
    else:
        c = MCPClient([sys.executable, str(HERE / "fastmcp_server.py")], "mcp-client")
        try:
            session(c)
        except RuntimeError as exc:  # e.g. an incompatible SDK version: explain, don't crash
            log("mcp-client", f"FastMCP part skipped - {exc}")
        finally:
            c.close()

    takeaway(
        "MCP = JSON-RPC 2.0 + transports (stdio, Streamable HTTP/SSE) + capability negotiation.",
        "Tools are discovered at runtime with JSON-Schema signatures: the LLM becomes the 'client stub'.",
        "One protocol, many hosts (Claude, IDEs, agents) and many servers (GitHub, DBs, SaaS...).",
        "Related: agent-to-agent protocols (A2A) for agents delegating tasks to other remote agents.",
        "Security matters: tools act on real systems -> auth (OAuth), least privilege, human approval.",
    )


if __name__ == "__main__":
    main()
