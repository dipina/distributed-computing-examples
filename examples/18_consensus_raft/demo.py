"""18 · Consensus & replication with Raft  [NEW].

How do several machines agree on ONE ordered log of commands even when some
crash or the network splits? That is **consensus**, the core of etcd (used by
Kubernetes), Consul, CockroachDB, TiKV, Kafka's KRaft, MongoDB replica sets...

This is a compact, teaching-oriented Raft (Ongaro & Ousterhout, 2014):

* **Leader election**: followers that stop hearing heartbeats become candidates,
  ask for votes, and a majority makes a leader for a new **term**.
* **Log replication**: clients talk to the leader; an entry is **committed** once
  stored on a majority; then every node applies it to its state machine (a KV store).
* **Safety**: a leader cut off in a minority partition can NOT commit anything;
  when the partition heals, its uncommitted entries are overwritten.

Nodes are threads exchanging messages through a simulated network that can
crash nodes and partition the cluster. (Simplifications: no persistence to disk,
no snapshots, no membership changes.)

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install pysyncobj       # a real Raft library for Python
    etcd (Raft-based KV store used by Kubernetes): https://etcd.io/docs/latest/install/
      etcdctl put city bilbao && etcdctl get city

Tutorials & references:
    - The Raft consensus algorithm (site + visualisation)
      https://raft.github.io/
    - Ongaro & Ousterhout, In Search of an Understandable Consensus Algorithm
      https://raft.github.io/raft.pdf
    - The Secret Lives of Data: Raft, animated
      https://thesecretlivesofdata.com/raft/
    - etcd documentation
      https://etcd.io/docs/
    - PySyncObj (Raft in Python)
      https://github.com/bakwc/PySyncObj
    - Jepsen: testing distributed systems' safety
      https://jepsen.io/

Run:  python examples/18_consensus_raft/demo.py
"""

from __future__ import annotations

import queue
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402

HEARTBEAT = 0.05
ELECTION_TIMEOUT = (0.3, 0.6)


@dataclass
class Entry:
    """A log entry: the term in which it was created and the command."""

    term: int
    cmd: tuple[str, float]


class Network:
    """Simulated network with crashes and partitions."""

    def __init__(self) -> None:
        """No nodes, no failures."""
        self.nodes: dict[str, "RaftNode"] = {}
        self.down: set[str] = set()
        self.groups: list[set[str]] | None = None

    def can_talk(self, a: str, b: str) -> bool:
        """True if a message from ``a`` can reach ``b``."""
        if a in self.down or b in self.down:
            return False
        return self.groups is None or any(a in g and b in g for g in self.groups)

    def send(self, src: str, dst: str, msg: dict[str, Any]) -> None:
        """Deliver ``msg`` unless the network forbids it."""
        if self.can_talk(src, dst):
            self.nodes[dst].inbox.put({**msg, "src": src})


@dataclass
class RaftNode:
    """One Raft server (single-threaded event loop)."""

    name: str
    net: Network
    peers: list[str]
    term: int = 0
    voted_for: str | None = None
    log_: list[Entry] = field(default_factory=list)
    commit: int = -1
    applied: int = -1
    state: str = "follower"
    leader: str | None = None
    kv: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.inbox: queue.Queue[dict[str, Any]] = queue.Queue()
        self.votes: set[str] = set()
        self.next_idx: dict[str, int] = {}
        self.match_idx: dict[str, int] = {}
        self.pending: dict[int, queue.Queue[str]] = {}
        self.deadline = self._new_deadline()
        self.next_hb = 0.0
        self.running = True
        threading.Thread(target=self._loop, daemon=True).start()

    # ------------------------------------------------------------- helpers
    def _new_deadline(self) -> float:
        return time.monotonic() + random.uniform(*ELECTION_TIMEOUT)

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _last(self) -> tuple[int, int]:
        return len(self.log_) - 1, self.log_[-1].term if self.log_ else 0

    def _send(self, dst: str, **msg: Any) -> None:
        self.net.send(self.name, dst, {"term": self.term, **msg})

    def _become(self, state: str) -> None:
        if state != self.state:
            log(self.name, f"{self.state} -> {state.upper()} (term {self.term})")
            if state == "follower":
                self.deadline = self._new_deadline()
        self.state = state

    # ------------------------------------------------------------- main loop
    def _loop(self) -> None:
        while self.running:
            try:
                msg = self.inbox.get(timeout=0.01)
            except queue.Empty:
                msg = None
            if self.name in self.net.down:
                continue  # crashed: do nothing
            if msg:
                self._handle(msg)
            now = time.monotonic()
            if self.state == "leader" and now >= self.next_hb:
                self._replicate()
                self.next_hb = now + HEARTBEAT
            elif self.state != "leader" and now >= self.deadline:
                self._start_election()

    # See: Raft paper section 5.2 (leader election), https://raft.github.io/raft.pdf
    def _start_election(self) -> None:
        self.term += 1
        self.voted_for, self.votes = self.name, {self.name}
        self._become("candidate")
        self.deadline = self._new_deadline()
        li, lt = self._last()
        for p in self.peers:
            self._send(p, type="RequestVote", last_index=li, last_term=lt)

    # See: Raft paper section 5.3 (log replication), https://raft.github.io/raft.pdf
    def _replicate(self) -> None:
        for p in self.peers:
            ni = self.next_idx[p]
            prev_term = self.log_[ni - 1].term if ni > 0 else 0
            entries = [(e.term, e.cmd) for e in self.log_[ni:]]
            self._send(p, type="AppendEntries", prev_index=ni - 1, prev_term=prev_term,
                       entries=entries, leader_commit=self.commit)

    def _apply(self) -> None:
        while self.applied < self.commit:
            self.applied += 1
            key, value = self.log_[self.applied].cmd
            self.kv[key] = value
            if self.applied in self.pending:
                self.pending.pop(self.applied).put("committed")

    # ------------------------------------------------------------- messages
    def _handle(self, m: dict[str, Any]) -> None:
        if m.get("term", 0) > self.term:  # newer term seen -> step down
            self.term, self.voted_for = m["term"], None
            self._become("follower")
        t = m["type"]
        if t == "RequestVote":
            li, lt = self._last()
            up_to_date = (m["last_term"], m["last_index"]) >= (lt, li)
            grant = m["term"] == self.term and self.voted_for in (None, m["src"]) and up_to_date
            if grant:
                self.voted_for = m["src"]
                self.deadline = self._new_deadline()
            self._send(m["src"], type="Vote", granted=grant)
        elif t == "Vote" and self.state == "candidate" and m["term"] == self.term and m["granted"]:
            self.votes.add(m["src"])
            if len(self.votes) >= self._majority():
                self._become("leader")
                self.leader = self.name
                log(self.name, f"won election with votes from {sorted(self.votes)}")
                self.next_idx = {p: len(self.log_) for p in self.peers}
                self.match_idx = {p: -1 for p in self.peers}
                self.next_hb = 0.0
        elif t == "AppendEntries":
            if m["term"] < self.term:
                self._send(m["src"], type="AppendReply", success=False, match=-1)
                return
            self._become("follower")
            self.leader, self.deadline = m["src"], self._new_deadline()
            pi = m["prev_index"]
            if pi >= len(self.log_) or (pi >= 0 and self.log_[pi].term != m["prev_term"]):
                self._send(m["src"], type="AppendReply", success=False, match=-1)
                return
            new = [Entry(tm, tuple(c)) for tm, c in m["entries"]]
            for k, e in enumerate(new):  # Raft rule: truncate at the first conflict, append the rest
                idx = pi + 1 + k
                if idx < len(self.log_) and self.log_[idx].term != e.term:
                    log(self.name, f"discarding uncommitted conflicting entries {[x.cmd for x in self.log_[idx:]]}")
                    self.log_ = self.log_[:idx]
                if idx >= len(self.log_):
                    self.log_.append(e)
            self.commit = max(self.commit, min(m["leader_commit"], len(self.log_) - 1))
            self._apply()
            self._send(m["src"], type="AppendReply", success=True, match=pi + len(new))
        elif t == "AppendReply" and self.state == "leader" and m["term"] == self.term:
            p = m["src"]
            if m["success"]:
                self.match_idx[p] = max(self.match_idx[p], m["match"])
                self.next_idx[p] = self.match_idx[p] + 1
                for n in range(len(self.log_) - 1, self.commit, -1):
                    replicas = 1 + sum(1 for q in self.peers if self.match_idx[q] >= n)
                    if self.log_[n].term == self.term and replicas >= self._majority():
                        self.commit = n
                        self._apply()
                        break
            else:
                self.next_idx[p] = max(0, self.next_idx[p] - 1)
        elif t == "Client":
            if self.state != "leader":
                m["reply"].put(f"not leader (try {self.leader})")
                return
            self.log_.append(Entry(self.term, tuple(m["cmd"])))
            self.pending[len(self.log_) - 1] = m["reply"]


class Cluster:
    """Convenience wrapper: build nodes, find the leader, submit commands."""

    def __init__(self, n: int = 5) -> None:
        """Create ``n`` nodes on a fresh network."""
        self.net = Network()
        names = [f"n{i}" for i in range(1, n + 1)]
        for name in names:
            self.net.nodes[name] = RaftNode(name, self.net, [p for p in names if p != name])

    def leader(self, among: set[str] | None = None, timeout: float = 5.0) -> RaftNode:
        """Wait until there is a live leader (optionally inside a node subset)."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            live = [n for n in self.net.nodes.values() if n.name not in self.net.down
                    and n.state == "leader" and (among is None or n.name in among)]
            if live:
                return max(live, key=lambda n: n.term)
            time.sleep(0.02)
        raise TimeoutError("no leader elected")

    def submit(self, node: RaftNode, key: str, value: float, timeout: float = 1.5) -> str:
        """Send a command to ``node`` and wait for the commit notification."""
        reply: queue.Queue[str] = queue.Queue()
        node.inbox.put({"type": "Client", "cmd": (key, value), "reply": reply, "term": 0})
        try:
            res = reply.get(timeout=timeout)
        except queue.Empty:
            res = f"NOT committed after {timeout}s (no majority)"
        log("client", f"set {key}={value} via {node.name} -> {res}")
        return res

    def show(self) -> None:
        """Print each node's term, role, log length and state machine."""
        for n in self.net.nodes.values():
            status = "DOWN" if n.name in self.net.down else n.state
            log(n.name, f"{status:<9} term={n.term} log={len(n.log_)} commit={n.commit} kv={n.kv}")

    def stop(self) -> None:
        """Stop all node threads."""
        for n in self.net.nodes.values():
            n.running = False


def main() -> None:
    """Run the Raft scenarios."""
    banner("18 · Consensus with Raft: leader election, replication, partitions", added=True)
    random.seed(7)
    c = Cluster(5)

    section("1) Leader election at start-up (randomised timeouts avoid split votes)")
    ld = c.leader()

    section("2) Replication: commands are committed once a MAJORITY stores them")
    c.submit(ld, "bilbao", 19.2)
    c.submit(ld, "madrid", 25.0)
    time.sleep(0.2)
    c.show()

    section("3) The leader crashes -> a new election, the service continues")
    c.net.down.add(ld.name)
    log("chaos", f"💥 {ld.name} crashed")
    ld2 = c.leader()
    c.submit(ld2, "oslo", 9.1)

    section("4) The old node comes back and catches up through log replication")
    c.net.down.discard(ld.name)
    ld.state, ld.deadline = "follower", ld._new_deadline()
    log("chaos", f"{ld.name} restarted")
    time.sleep(0.4)
    c.show()

    section("5) Network partition: leader + 1 node (minority) vs 3 nodes (majority)")
    ld = c.leader()
    others = [n for n in c.net.nodes if n != ld.name]
    minority, majority = {ld.name, others[0]}, set(others[1:])
    c.net.groups = [minority, majority]
    log("chaos", f"✂ partition {sorted(minority)} | {sorted(majority)}")
    c.submit(ld, "barcelona", 99.9, timeout=0.8)  # the old leader cannot reach a majority
    new = c.leader(among=majority)
    c.submit(new, "barcelona", 23.0)

    section("6) Heal the partition: the stale leader steps down and its uncommitted entry is discarded")
    c.net.groups = None
    log("chaos", "partition healed")
    time.sleep(0.6)
    c.show()
    kvs = [n.kv for n in c.net.nodes.values()]
    log("check", f"all 5 state machines identical: {all(k == kvs[0] for k in kvs)}")
    c.stop()

    takeaway(
        "Consensus = agreeing on one ordered log despite crashes and partitions.",
        "Majorities (quorums) are the trick: any two majorities overlap, so there can't be two histories.",
        "A minority side stays available for reads at most; it cannot commit writes (CAP: Raft picks C over A).",
        "This is the heart of etcd/Kubernetes, Consul, CockroachDB, KRaft... (5 nodes tolerate 2 failures).",
    )


if __name__ == "__main__":
    main()
