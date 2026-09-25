"""03 · Peer-to-peer paradigm (slide 6): a mini-IPFS.

There is no client and no server: every **peer** runs a small TCP server
*and* acts as a client of the others. Like IPFS we use:

* **Content addressing** - a block is identified by its hash (CID = SHA-256),
  so any peer can serve it and the receiver can verify it.
* **Chunking + a manifest** - a file is split in blocks; a manifest block lists them.
* **A DHT (Kademlia flavour)** - "who has CID X?" records are stored on the peers
  whose ID is closest (XOR distance) to X, so no central index is needed.

Story: Alice shares a file, Bob downloads it (and starts seeding), Alice leaves,
Carol still gets the file from Bob, and Mallory's corrupted blocks are rejected.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Try real IPFS (Kubo): https://docs.ipfs.tech/install/command-line/
      ipfs init && ipfs add README.md && ipfs cat <CID>

Tutorials & references:
    - IPFS docs: Content Identifiers (CIDs)
      https://docs.ipfs.tech/concepts/content-addressing/
    - IPFS: basic CLI operations with Kubo
      https://docs.ipfs.tech/how-to/kubo-basic-cli/
    - Maymounkov & Mazieres, Kademlia (IPTPS 2002)
      https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf
    - libp2p: modular peer-to-peer networking stack
      https://libp2p.io/
    - hashlib (SHA-256)
      https://docs.python.org/3/library/hashlib.html

Run:  python examples/03_peer_to_peer/demo.py
"""

from __future__ import annotations

import base64
import hashlib
import json
import socket
import socketserver
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, start_thread, takeaway  # noqa: E402

CHUNK_SIZE = 64
K_CLOSEST = 2  # replication factor of provider records in the DHT


# See: https://docs.ipfs.tech/concepts/content-addressing/ (real CIDs are multihashes)
def cid_of(data: bytes) -> str:
    """Content identifier: the SHA-256 of the bytes."""
    return hashlib.sha256(data).hexdigest()


def rpc(port: int, msg: dict[str, Any]) -> dict[str, Any]:
    """Send one JSON message to the peer on ``port`` and return its reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall((json.dumps(msg) + "\n").encode())
        return json.loads(s.makefile().readline())


class Peer:
    """A node that is simultaneously server and client."""

    def __init__(self, name: str, evil: bool = False) -> None:
        """Start the peer's TCP server.

        Args:
            name: Peer name (its ID is the hash of the name).
            evil: If True the peer serves corrupted blocks (to show verification).
        """
        self.name = name
        self.id = int(cid_of(name.encode()), 16)
        self.evil = evil
        self.blocks: dict[str, bytes] = {}
        self.providers: dict[str, list[int]] = {}  # this peer's slice of the DHT
        self.routing: dict[int, int] = {}  # peer id -> port (the known peers)
        peer = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                msg = json.loads(self.rfile.readline())
                self.wfile.write((json.dumps(peer.on_message(msg)) + "\n").encode())

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port: int = self.server.server_address[1]
        start_thread(lambda: self.server.serve_forever(poll_interval=0.05))
        self.online = True

    # ----------------------------------------------------------- server side
    def on_message(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Handle an incoming message from another peer."""
        kind, cid = msg["type"], msg.get("cid", "")
        if kind == "ADD_PROVIDER":
            provs = self.providers.setdefault(cid, [])
            if msg["port"] in provs:
                provs.remove(msg["port"])
            provs.append(msg["port"])
            return {"ok": True}
        if kind == "GET_PROVIDERS":  # freshest announcement first
            return {"providers": list(reversed(self.providers.get(cid, [])))}
        if kind == "GET_BLOCK" and cid in self.blocks:
            data = b"garbage!" if self.evil else self.blocks[cid]
            return {"data": base64.b64encode(data).decode()}
        return {"error": "not found"}

    # ----------------------------------------------------------- client side
    def join(self, others: list["Peer"]) -> None:
        """Bootstrap: learn about other peers."""
        self.routing = {p.id: p.port for p in others}

    # See: Kademlia XOR metric, https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf
    def closest(self, cid: str) -> list[int]:
        """Ports of the K peers whose ID is XOR-closest to ``cid`` (Kademlia metric)."""
        key = int(cid, 16)
        return [self.routing[i] for i in sorted(self.routing, key=lambda i: i ^ key)[:K_CLOSEST]]

    def provide(self, cid: str) -> None:
        """Announce in the DHT that this peer holds ``cid``."""
        for port in self.closest(cid):
            try:
                rpc(port, {"type": "ADD_PROVIDER", "cid": cid, "port": self.port})
            except OSError:
                pass

    def find_providers(self, cid: str) -> list[int]:
        """Look up who holds ``cid`` by asking the closest peers (freshest first)."""
        found: list[int] = []
        for port in self.closest(cid):
            try:
                for prov in rpc(port, {"type": "GET_PROVIDERS", "cid": cid})["providers"]:
                    if prov not in found and prov != self.port:
                        found.append(prov)
            except OSError:
                log(self.name, f"DHT node on :{port} unreachable, trying others")
        return found

    def add_file(self, content: bytes) -> str:
        """Chunk, store and announce a file. Returns the root CID (of the manifest)."""
        chunks = [content[i:i + CHUNK_SIZE] for i in range(0, len(content), CHUNK_SIZE)]
        cids = []
        for ch in chunks:
            cids.append(cid_of(ch))
            self.blocks[cids[-1]] = ch
        manifest = json.dumps({"chunks": cids}).encode()
        root = cid_of(manifest)
        self.blocks[root] = manifest
        for c in [root, *cids]:
            self.provide(c)
        log(self.name, f"added file: {len(chunks)} chunks, root CID {root[:12]}…")
        return root

    def fetch_block(self, cid: str) -> bytes:
        """Download a block from any provider and verify its hash."""
        if cid in self.blocks:
            return self.blocks[cid]
        for port in self.find_providers(cid):
            try:
                data = base64.b64decode(rpc(port, {"type": "GET_BLOCK", "cid": cid})["data"])
            except (OSError, KeyError):
                log(self.name, f"provider :{port} unreachable for {cid[:8]}, next one")
                continue
            if cid_of(data) != cid:
                log(self.name, f"⚠ block {cid[:8]} from :{port} FAILED hash check -> rejected")
                continue
            self.blocks[cid] = data
            self.provide(cid)  # now I can seed it too
            return data
        raise LookupError(f"no honest provider for {cid[:12]}")

    def get_file(self, root: str) -> bytes:
        """Fetch a whole file by its root CID."""
        manifest = json.loads(self.fetch_block(root))
        data = b"".join(self.fetch_block(c) for c in manifest["chunks"])
        log(self.name, f"downloaded {len(data)} bytes, verified {len(manifest['chunks'])} chunks")
        return data

    def leave(self) -> None:
        """Go offline."""
        self.server.shutdown()
        self.server.server_close()
        log(self.name, "went OFFLINE")


def main() -> None:
    """Run the P2P story."""
    banner("03 · Peer-to-peer: content addressing + DHT (mini-IPFS)", "6")
    names = ["alice", "bob", "carol", "dave", "erin"]
    peers = {n: Peer(n) for n in names}
    mallory = Peer("mallory", evil=True)
    everyone = [*peers.values(), mallory]
    for p in everyone:
        p.join(everyone)
    log("network", "peers: " + ", ".join(f"{p.name}:{p.port}" for p in everyone))

    section("Alice shares a file; the DHT stores 'who has it' on the closest peers")
    text = ("Distributed computing: a collection of independent computers that appears "
            "to its users as a single coherent system. (Tanenbaum & van Steen)").encode()
    root = peers["alice"].add_file(text)

    section("Bob downloads it by CID only (no server, no URL) and becomes a seeder")
    assert peers["bob"].get_file(root) == text

    section("Alice leaves the network - the content survives on Bob")
    peers["alice"].leave()
    peers["alice"].routing.clear()

    section("Mallory also claims to have the blocks, but serves garbage")
    for c in json.loads(peers["bob"].blocks[root])["chunks"][:2]:
        mallory.blocks[c] = b"x"
        mallory.provide(c)
    got = peers["carol"].get_file(root)
    log("carol", f"content: {got.decode()[:60]}…")
    assert got == text

    for p in everyone:
        if p.name != "alice":
            p.leave()
    takeaway(
        "Every peer is both client and server; any peer can join or leave at any time.",
        "Content addressing (CID = hash) lets you download from untrusted peers and verify integrity.",
        "A DHT spreads the 'index' over the peers themselves (no central tracker).",
        "Popular content becomes MORE available as more peers download and re-seed it.",
    )


if __name__ == "__main__":
    main()
