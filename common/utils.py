"""Shared helpers for every demo: pretty logging, ports, optional deps, servers.

Nothing here is specific to one paradigm; it only keeps the demos short.
"""

from __future__ import annotations

import importlib
import os
import socket
import sys
import threading
import time
from types import ModuleType
from typing import Any, Callable

#: Exit code that tells ``run_all.py`` an example was skipped (autotools convention).
SKIP_EXIT_CODE: int = 77

#: Shared start time (inherited by child processes through the environment).
_START: float = float(os.environ.setdefault("DEMO_T0", str(time.time())))
_PRINT_LOCK = threading.Lock()
# Never crash on non-ASCII symbols when output is redirected on Windows (cp1252 consoles/pipes).
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(errors="replace")
_USE_COLOR: bool = sys.stdout.isatty() and not os.environ.get("NO_COLOR")

_PALETTE: list[str] = ["36", "33", "35", "32", "34", "31", "96", "93", "95", "92"]
_ACTOR_COLORS: dict[str, str] = {}


def _c(code: str, text: str) -> str:
    """Wrap ``text`` in an ANSI colour code when colours are enabled."""
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def banner(title: str, slides: str = "", added: bool = False) -> None:
    """Print the header of a demo.

    Args:
        title: Human readable title of the paradigm.
        slides: Slide numbers of Unit 0 that the demo illustrates.
        added: True for paradigms that are *not* in the slides (cutting-edge additions).
    """
    tag = "  [NEW: not in slides]" if added else (f"  [slides {slides}]" if slides else "")
    line = "=" * 78
    print(_c("1", line))
    print(_c("1", f" {title}{tag}"))
    print(_c("1", line), flush=True)


def section(text: str) -> None:
    """Print a sub-heading inside a demo."""
    print("\n" + _c("1;4", f"▶ {text}"), flush=True)


def log(actor: str, msg: str) -> None:
    """Thread-safe, time-stamped, colour-per-actor log line.

    Args:
        actor: Name of the process/thread/node that "speaks".
        msg: What happened.
    """
    if actor not in _ACTOR_COLORS:
        _ACTOR_COLORS[actor] = _PALETTE[len(_ACTOR_COLORS) % len(_PALETTE)]
    elapsed = time.time() - _START
    with _PRINT_LOCK:
        print(f"{elapsed:6.2f}s {_c(_ACTOR_COLORS[actor], f'[{actor:>12}]')} {msg}", flush=True)


def takeaway(*points: str) -> None:
    """Print the key lessons of a demo."""
    print("\n" + _c("1;32", "Key takeaways:"))
    for p in points:
        print(f"  • {p}")
    print(flush=True)


def free_port() -> int:
    """Return a TCP port that is currently free on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_for_port(port: int, host: str = "127.0.0.1", timeout: float = 10.0) -> None:
    """Block until something accepts TCP connections on ``host:port``.

    Raises:
        TimeoutError: If nothing listens before ``timeout`` seconds.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Nothing listening on {host}:{port} after {timeout}s")


def require(module: str, pip_name: str | None = None) -> ModuleType:
    """Import an optional dependency or exit with :data:`SKIP_EXIT_CODE`.

    Args:
        module: Importable module name (e.g. ``"fastapi"``).
        pip_name: Package name to suggest to the user if missing.

    Returns:
        The imported module.
    """
    try:
        return importlib.import_module(module)
    except ImportError:
        pkg = pip_name or module
        print(f"SKIPPED: optional dependency '{module}' not installed. Run: pip install {pkg}")
        sys.exit(SKIP_EXIT_CODE)


def start_thread(target: Callable[..., Any], *args: Any, name: str | None = None) -> threading.Thread:
    """Start a daemon thread running ``target(*args)`` and return it."""
    t = threading.Thread(target=target, args=args, name=name, daemon=True)
    t.start()
    return t


class UvicornThread:
    """Run an ASGI app (FastAPI, Strawberry...) with uvicorn in a background thread.

    Example:
        >>> with UvicornThread(app) as srv:
        ...     httpx.get(srv.url + "/")
    """

    def __init__(self, app: Any, port: int | None = None) -> None:
        """Create the server (not yet started).

        Args:
            app: ASGI application.
            port: TCP port; a free one is chosen when omitted.
        """
        import uvicorn  # local import: optional dependency

        self.port: int = port or free_port()
        self.url: str = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> "UvicornThread":
        self._thread.start()
        wait_for_port(self.port)
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)
