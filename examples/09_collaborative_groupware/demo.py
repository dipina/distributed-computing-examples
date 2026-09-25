"""09 · Collaborative application (groupware) paradigm (slide 14).

Processes join a **session/group** and every member can contribute. Two styles:

* **Message-based groupware** - a member *multicasts* a message to the whole
  group (or a subset). We use a *sequencer* server that stamps every message
  with a global sequence number -> all members see the SAME order
  (total-order multicast), which is what keeps a chat/editor consistent.
* **Whiteboard-based groupware** - a shared board that anyone can read/write.
  Late joiners receive a snapshot of the current board.

Each member is a TCP client (thread) connected to the group server.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)

Tutorials & references:
    - Atomic (total-order) broadcast
      https://en.wikipedia.org/wiki/Atomic_broadcast
    - Collaborative software / groupware
      https://en.wikipedia.org/wiki/Collaborative_software
    - socketserver
      https://docs.python.org/3/library/socketserver.html
    - WebRTC (browser peer-to-peer collaboration)
      https://webrtc.org/

Run:  python examples/09_collaborative_groupware/demo.py
"""

from __future__ import annotations

import json
import socket
import socketserver
import sys
import threading
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, start_thread, takeaway  # noqa: E402


class GroupServer:
    """Sequencer for total-order multicast + holder of the shared whiteboard."""

    def __init__(self) -> None:
        """Start listening."""
        self.members: dict[str, Any] = {}  # name -> wfile
        self.board: dict[str, str] = {}  # cell -> value
        self.seq = 0
        self.lock = threading.Lock()
        server = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                name = json.loads(self.rfile.readline())["join"]
                with server.lock:
                    server.members[name] = self.wfile
                    self._send({"type": "welcome", "board": dict(server.board), "members": sorted(server.members)})
                server.multicast({"type": "system", "text": f"{name} joined"})
                for line in self.rfile:
                    server.multicast({**json.loads(line), "from": name})
                with server.lock:
                    server.members.pop(name, None)

            def _send(self, msg: dict[str, Any]) -> None:
                self.wfile.write((json.dumps(msg) + "\n").encode())

        self.srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.srv.daemon_threads = True
        self.port: int = self.srv.server_address[1]
        start_thread(lambda: self.srv.serve_forever(poll_interval=0.05))

    def multicast(self, msg: dict[str, Any]) -> None:
        """Stamp with a sequence number, apply to the board, send to the group."""
        with self.lock:  # the lock makes the sequencer order = delivery order
            self.seq += 1
            msg["seq"] = self.seq
            if msg["type"] == "draw":
                self.board[msg["cell"]] = msg["value"]
            to = msg.get("to") or list(self.members)
            data = (json.dumps(msg) + "\n").encode()
            for name in to:
                if name in self.members:
                    self.members[name].write(data)


class Member:
    """A participant of the collaborative session."""

    def __init__(self, name: str, port: int) -> None:
        """Connect and join the group."""
        self.name = name
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.rf = self.sock.makefile("r")
        self.sock.sendall((json.dumps({"join": name}) + "\n").encode())
        welcome = json.loads(self.rf.readline())
        self.board: dict[str, str] = welcome["board"]
        self.delivered: list[int] = []
        self.chat: list[str] = []
        log(name, f"joined; members={welcome['members']}; board snapshot={self.board}")
        start_thread(self._listen)

    def _listen(self) -> None:
        for line in self.rf:
            m = json.loads(line)
            self.delivered.append(m["seq"])
            if m["type"] == "draw":
                self.board[m["cell"]] = m["value"]
            elif m["type"] == "chat":
                self.chat.append(f"{m['from']}: {m['text']}")
                scope = f" (private to {m['to']})" if m.get("to") else ""
                log(self.name, f"#{m['seq']} {m['from']} says '{m['text']}'{scope}")

    def say(self, text: str, to: list[str] | None = None) -> None:
        """Multicast a chat message to the group (or to a subset)."""
        self.sock.sendall((json.dumps({"type": "chat", "text": text, "to": to}) + "\n").encode())

    def draw(self, cell: str, value: str) -> None:
        """Write on the shared whiteboard."""
        self.sock.sendall((json.dumps({"type": "draw", "cell": cell, "value": value}) + "\n").encode())

    def leave(self) -> None:
        """Close the connection."""
        self.sock.close()


def main() -> None:
    """Run the groupware session."""
    banner("09 · Collaborative groupware: multicast + shared whiteboard", "14")
    server = GroupServer()

    section("Message-based groupware: multicast to the group, total order via a sequencer")
    ana, ben, cai = (Member(n, server.port) for n in ("ana", "ben", "cai"))
    time.sleep(0.1)
    threads = [threading.Thread(target=m.say, args=(f"hello from {m.name}",)) for m in (ana, ben, cai)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ana.say("psst, only for ben", to=["ben", "ana"])
    time.sleep(0.2)
    same = ana.chat[:3] == ben.chat[:3] == cai.chat[:3]
    log("check", f"all members saw the 3 concurrent messages in the SAME order: {same}")

    section("Whiteboard-based groupware: everyone reads/writes a shared board")
    ana.draw("A1", "Bilbao 19°C")
    ben.draw("B1", "Madrid 26°C")
    cai.draw("A1", "Bilbao 21°C (updated)")
    time.sleep(0.2)
    log("check", f"ana's board == ben's board == cai's board: {ana.board == ben.board == cai.board} -> {ana.board}")

    section("A late joiner gets the current board as a snapshot")
    dan = Member("dan", server.port)
    time.sleep(0.1)
    log("check", f"dan's board: {dan.board}")
    for m in (ana, ben, cai, dan):
        m.leave()

    takeaway(
        "Groupware = many-to-many: every participant is producer and consumer.",
        "Concurrent multicasts can arrive in different orders; a sequencer gives total order.",
        "Shared-state (whiteboard) groupware needs snapshots for late joiners.",
        "The central sequencer is simple but a bottleneck/SPOF: CRDTs remove it (see 19).",
    )


if __name__ == "__main__":
    main()
