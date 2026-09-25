"""21 · Durable workflows (Temporal-style) and the Saga pattern  [NEW].

A business process that spans several microservices cannot use one ACID
transaction. Two modern answers, combined here:

* **Saga pattern** - a sequence of local steps, each with a **compensating
  action**. If step N fails, steps N-1..1 are undone in reverse order.
* **Durable execution** (Temporal, Cadence, AWS Step Functions, Azure Durable
  Functions, Restate, DBOS) - the workflow is plain code, but every activity
  result is appended to a persistent **event history**. If the worker process
  crashes, another one **replays** the history: finished activities are NOT
  executed again, and the code continues exactly where it stopped.
  Activities get automatic **retries with exponential backoff**.

Scenario: booking a weather-drone mission = reserve drone -> charge customer
-> get flight permit. Side effects are logged to a file so we can prove
nothing is executed twice.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Real durable execution with Temporal:
      pip install temporalio
      Temporal CLI: https://docs.temporal.io/cli/setup-cli   (brew install temporal)
      temporal server start-dev      # local dev server + web UI on http://localhost:8233

Tutorials & references:
    - microservices.io: Saga pattern
      https://microservices.io/patterns/data/saga.html
    - Garcia-Molina & Salem, Sagas (SIGMOD 1987)
      https://dl.acm.org/doi/10.1145/38713.38742
    - Temporal Python SDK developer guide
      https://docs.temporal.io/develop/python
    - Temporal: set up your local environment (Python)
      https://docs.temporal.io/develop/python/set-up-your-local-python
    - Temporal: run a development server
      https://docs.temporal.io/develop/run-a-development-server
    - Azure Durable Functions
      https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-overview
    - AWS Step Functions
      https://docs.aws.amazon.com/step-functions/

Run:  python examples/21_durable_workflows_saga/demo.py
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402


class ActivityFailed(Exception):
    """Raised when an activity keeps failing after all retries."""


class Workflow:
    """Durable workflow context backed by an append-only JSON-lines history file."""

    def __init__(self, workdir: Path, wf_id: str) -> None:
        """Load the existing history (if any) for replay."""
        self.wf_id = wf_id
        self.history_file = workdir / f"{wf_id}.history.jsonl"
        self.effects_file = workdir / "external_side_effects.log"
        self.history = [json.loads(l) for l in self.history_file.read_text().splitlines()] if self.history_file.exists() else []
        self.cursor = 0
        self.compensations: list[tuple[str, Callable[..., Any], tuple[Any, ...]]] = []
        self.who = f"worker-{os.getpid()}"

    def _record(self, event: dict[str, Any]) -> None:
        with self.history_file.open("a") as fh:
            fh.write(json.dumps(event) + "\n")
            fh.flush()
            os.fsync(fh.fileno())  # durable before we move on

    def effect(self, text: str) -> None:
        """Simulate an external side effect (API call, payment, e-mail...)."""
        with self.effects_file.open("a") as fh:
            fh.write(f"{self.wf_id}: {text}\n")

    # See: how Temporal replays history, https://docs.temporal.io/develop/python
    def activity(self, name: str, fn: Callable[..., Any], *args: Any, retries: int = 3,
                 compensate: Callable[..., Any] | None = None) -> Any:
        """Run (or replay) one activity."""
        if self.cursor < len(self.history):  # REPLAY: result comes from the history
            ev = self.history[self.cursor]
            assert ev["name"] == name, "non-deterministic workflow code!"
            self.cursor += 1
            log(self.who, f"↺ replay {name} -> {ev['result']} (not executed again)")
            result = ev["result"]
        else:
            delay = 0.05
            for attempt in range(1, retries + 1):
                try:
                    result = fn(self, *args)
                    break
                except Exception as exc:  # noqa: BLE001
                    log(self.who, f"✗ {name} attempt {attempt} failed: {exc}")
                    if attempt == retries:
                        raise ActivityFailed(f"{name}: {exc}") from exc
                    time.sleep(delay)
                    delay *= 2  # exponential backoff
            self._record({"name": name, "result": result})
            self.history.append({"name": name, "result": result})
            self.cursor += 1
            log(self.who, f"✓ {name} -> {result}")
        if compensate:
            self.compensations.append((f"undo_{name}", compensate, (result,)))
        return result

    # See: https://microservices.io/patterns/data/saga.html
    def compensate_all(self) -> None:
        """Saga rollback: run compensations in reverse order (durably, too)."""
        for name, fn, args in reversed(self.compensations):
            self.activity(name, lambda wf, *a, f=fn: f(wf, *a), *args)


# ============================================================ activities (talk to "other services")
FLAKY_STATE: dict[str, int] = {}


def reserve_drone(wf: Workflow, city: str) -> str:
    """Reserve a drone in the fleet service."""
    wf.effect(f"drone reserved for {city}")
    return f"drone-{sum(map(ord, city)) % 100:02d}"


def release_drone(wf: Workflow, drone: str) -> str:
    """Compensation of :func:`reserve_drone`."""
    wf.effect(f"drone {drone} released")
    return "released"


def charge_customer(wf: Workflow, amount: float) -> str:
    """Charge via the payment service (flaky: fails the first 2 times in some runs)."""
    if os.environ.get("FLAKY_PAYMENTS") and FLAKY_STATE.setdefault("n", 0) < 2:
        FLAKY_STATE["n"] += 1
        raise ConnectionError("payment gateway 503")
    wf.effect(f"customer charged {amount}€")
    return f"payment-{int(amount * 100)}"


def refund_customer(wf: Workflow, payment: str) -> str:
    """Compensation of :func:`charge_customer`."""
    wf.effect(f"refund of {payment}")
    return "refunded"


def request_permit(wf: Workflow, city: str) -> str:
    """Ask the aviation authority for a permit (always denied for Oslo)."""
    if city == "oslo":
        raise PermissionError("airspace closed (storm)")
    wf.effect(f"permit granted for {city}")
    return f"permit-{city}"


def drone_mission(workdir: Path, wf_id: str, city: str, crash_after: str | None = None) -> str:
    """THE WORKFLOW: ordinary-looking sequential code, made durable by the context."""
    wf = Workflow(workdir, wf_id)
    try:
        drone = wf.activity("reserve_drone", reserve_drone, city, compensate=release_drone)
        if crash_after == "reserve_drone":
            log(wf.who, "💥 worker process crashes (power cut)!")
            os._exit(1)
        payment = wf.activity("charge_customer", charge_customer, 49.90, compensate=refund_customer)
        permit = wf.activity("request_permit", request_permit, city, retries=2)
        return f"COMPLETED: {drone}, {payment}, {permit}"
    except ActivityFailed as exc:
        log(wf.who, f"saga step failed ({exc}) -> compensating in reverse order")
        wf.compensate_all()
        return "COMPENSATED (rolled back)"


def effects(workdir: Path, wf_id: str) -> list[str]:
    """Side effects actually performed for one workflow."""
    f = workdir / "external_side_effects.log"
    return [l.split(": ", 1)[1] for l in f.read_text().splitlines() if l.startswith(wf_id)] if f.exists() else []


def main() -> None:
    """Run the four scenarios."""
    banner("21 · Durable workflows + Saga pattern (Temporal-style)", added=True)
    with tempfile.TemporaryDirectory() as tmp:
        wd = Path(tmp)

        section("1) Happy path")
        log("client", drone_mission(wd, "wf-bilbao", "bilbao"))

        section("2) Saga: the last step fails permanently -> compensations run in reverse")
        log("client", drone_mission(wd, "wf-oslo", "oslo"))
        log("audit", f"side effects for wf-oslo: {effects(wd, 'wf-oslo')}")

        section("3) Durable execution: the worker CRASHES mid-workflow, another worker resumes it")
        p = mp.Process(target=drone_mission, args=(wd, "wf-madrid", "madrid", "reserve_drone"))
        p.start()
        p.join()
        log("orchestrator", f"worker exited with code {p.exitcode}; history has "
                            f"{len((wd / 'wf-madrid.history.jsonl').read_text().splitlines())} event(s). Rescheduling...")
        p2 = mp.Pool(1)
        log("client", p2.apply(drone_mission, (wd, "wf-madrid", "madrid")))
        p2.close()
        log("audit", f"side effects for wf-madrid: {effects(wd, 'wf-madrid')}  <- drone reserved ONCE")

        section("4) Transient failures: automatic retries with exponential backoff")
        os.environ["FLAKY_PAYMENTS"] = "1"
        log("client", drone_mission(wd, "wf-barcelona", "barcelona"))
        del os.environ["FLAKY_PAYMENTS"]

    takeaway(
        "No distributed ACID transactions across services: use Sagas (local steps + compensations).",
        "Durable execution persists every step, so workflows survive crashes and restarts transparently.",
        "Replay requires deterministic workflow code; side effects live only in activities.",
        "Retries/backoff/timeouts become platform concerns (Temporal, Step Functions, Durable Functions, Restate).",
    )


if __name__ == "__main__":
    main()
