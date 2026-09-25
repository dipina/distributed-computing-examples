"""16 · Serverless / Function-as-a-Service (slide 72).

We build a toy FaaS platform (think AWS Lambda, Google Cloud Run functions,
Azure Functions, OpenFaaS, Knative) to make its mechanics visible:

* Developers deploy **functions**, not servers; the platform runs them in
  isolated **execution environments** (here: OS processes = "containers").
* **Event triggers**: HTTP request, message queue, schedule (cron).
* **Cold start** (create env + init runtime) vs **warm start** (reuse env).
* **Autoscaling**: concurrent events -> more environments (1 request each).
* **Scale to zero**: idle environments are reclaimed -> you pay nothing.
* **Pay per use**: billed in GB-seconds of actual execution.
* **Timeouts** and **statelessness** (in-memory globals are NOT reliable state).

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Try real FaaS locally: pip install functions-framework   (Google Cloud Run functions)
                           functions-framework --target=<function_name>
    AWS SAM CLI: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html
                 sam init && sam local invoke

Tutorials & references:
    - AWS Lambda: execution environment lifecycle (cold starts)
      https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html
    - AWS Lambda: function scaling
      https://docs.aws.amazon.com/lambda/latest/dg/lambda-concurrency.html
    - AWS Lambda pricing (GB-seconds)
      https://aws.amazon.com/lambda/pricing/
    - Azure Functions Python developer guide
      https://learn.microsoft.com/azure/azure-functions/functions-reference-python
    - OpenFaaS (open-source FaaS on Kubernetes)
      https://docs.openfaas.com/
    - Knative (serverless on Kubernetes)
      https://knative.dev/docs/

Run:  python examples/16_serverless_faas/demo.py
"""

from __future__ import annotations

import itertools
import multiprocessing as mp
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, log, section, takeaway  # noqa: E402

COLD_START_INIT = 0.4  # seconds to boot the runtime + import dependencies
IDLE_TIMEOUT = 1.0  # scale-to-zero after this idle time
PRICE_PER_GB_S = 0.0000166667  # ~ AWS Lambda x86 list price


# ======================================================= user functions (the "code you deploy")
_warm_counter = 0  # module global: survives ONLY while the same environment stays warm


def to_fahrenheit(event: dict[str, Any]) -> dict[str, Any]:
    """HTTP-triggered function."""
    global _warm_counter
    _warm_counter += 1
    time.sleep(0.1)
    return {"city": event["city"], "fahrenheit": round(event["celsius"] * 9 / 5 + 32, 1),
            "invocations_seen_by_this_env": _warm_counter}


def on_new_reading(event: dict[str, Any]) -> dict[str, Any]:
    """Queue-triggered function: react to a message."""
    time.sleep(0.2)
    return {"alert": event["celsius"] >= 35, "city": event["city"]}


def hourly_report(event: dict[str, Any]) -> str:
    """Schedule-triggered function."""
    return f"report generated at {time.strftime('%H:%M:%S')}"


def buggy_infinite_loop(event: dict[str, Any]) -> None:
    """A function that never ends (to show platform timeouts)."""
    while True:
        time.sleep(0.1)


# ======================================================= the platform
# See: https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html (init vs invoke phases)
def environment_main(conn: Connection, handler: Callable[[dict[str, Any]], Any]) -> None:
    """Code running inside one execution environment (container)."""
    time.sleep(COLD_START_INIT)  # runtime boot + imports
    conn.send("ready")
    while True:
        event = conn.recv()
        t0 = time.perf_counter()
        result = handler(event)
        conn.send((result, time.perf_counter() - t0))


@dataclass
class Environment:
    """A running execution environment for one function."""

    id: int
    proc: mp.Process
    conn: Connection
    busy: bool = False
    last_used: float = field(default_factory=time.monotonic)


@dataclass
class FunctionSpec:
    """A deployed function."""

    name: str
    handler: Callable[[dict[str, Any]], Any]
    memory_mb: int = 128
    timeout: float = 2.0
    envs: list[Environment] = field(default_factory=list)


class FaaSPlatform:
    """Schedules invocations onto warm or new environments; reaps idle ones."""

    def __init__(self) -> None:
        """Start the reaper thread."""
        self.functions: dict[str, FunctionSpec] = {}
        self.lock = threading.Lock()
        self.ids = itertools.count(1)
        self.bill_gb_s = 0.0
        self._stop = threading.Event()
        threading.Thread(target=self._reaper, daemon=True).start()

    def deploy(self, name: str, handler: Callable[[dict[str, Any]], Any], memory_mb: int = 128, timeout: float = 2.0) -> None:
        """Register a function. No server is started until an event arrives."""
        self.functions[name] = FunctionSpec(name, handler, memory_mb, timeout)
        log("platform", f"deployed '{name}' ({memory_mb} MB, timeout {timeout}s) - 0 environments running")

    def _acquire(self, fn: FunctionSpec) -> tuple[Environment, bool]:
        with self.lock:
            for env in fn.envs:
                if not env.busy:
                    env.busy = True
                    return env, False
            parent, child = mp.Pipe()
            proc = mp.Process(target=environment_main, args=(child, fn.handler), daemon=True)
            env = Environment(next(self.ids), proc, parent, busy=True)
            fn.envs.append(env)
        proc.start()
        env.conn.recv()  # wait for "ready" (cold start)
        return env, True

    def invoke(self, name: str, event: dict[str, Any], trigger: str = "http") -> Any:
        """Handle one event: pick/create an environment, run, bill."""
        fn = self.functions[name]
        t0 = time.perf_counter()
        env, cold = self._acquire(fn)
        env.conn.send(event)
        if not env.conn.poll(fn.timeout):
            env.proc.terminate()
            with self.lock:
                fn.envs.remove(env)
            log("platform", f"{name}: ⏱ TIMEOUT after {fn.timeout}s -> environment #{env.id} killed")
            return None
        result, run_s = env.conn.recv()
        total = time.perf_counter() - t0
        billed_ms = max(1, round(run_s * 1000))  # per-ms billing of execution time only
        with self.lock:
            self.bill_gb_s += fn.memory_mb / 1024 * billed_ms / 1000
            env.busy, env.last_used = False, time.monotonic()
        log(f"{trigger} trigger", f"{name} env#{env.id} {'COLD' if cold else 'warm'} latency={total * 1000:4.0f} ms "
                                      f"billed={billed_ms} ms -> {result}")
        return result

    def running(self) -> int:
        """Number of live environments across all functions."""
        return sum(len(f.envs) for f in self.functions.values())

    def _reaper(self) -> None:
        while not self._stop.wait(0.2):
            with self.lock:
                for fn in self.functions.values():
                    for env in [e for e in fn.envs if not e.busy and time.monotonic() - e.last_used > IDLE_TIMEOUT]:
                        env.proc.terminate()
                        fn.envs.remove(env)
                        log("platform", f"scale-to-zero: reclaimed idle env#{env.id} of '{fn.name}'")

    def shutdown(self) -> None:
        """Stop everything."""
        self._stop.set()
        for fn in self.functions.values():
            for env in fn.envs:
                env.proc.terminate()


def main() -> None:
    """Run the FaaS scenarios."""
    banner("16 · Serverless / FaaS: cold starts, autoscaling, scale-to-zero, pay-per-use", "72")
    faas = FaaSPlatform()
    faas.deploy("to_fahrenheit", to_fahrenheit)
    faas.deploy("on_new_reading", on_new_reading, memory_mb=256)
    faas.deploy("hourly_report", hourly_report)
    faas.deploy("buggy", buggy_infinite_loop, timeout=0.5)

    section("HTTP trigger: first call is a COLD start, the next ones are warm (and reuse globals!)")
    for c in (18.0, 25.0, 30.0):
        faas.invoke("to_fahrenheit", {"city": "bilbao", "celsius": c})

    section("Burst of 5 concurrent queue events -> the platform scales out automatically")
    with ThreadPoolExecutor(5) as pool:
        list(pool.map(lambda t: faas.invoke("on_new_reading", {"city": "madrid", "celsius": t}, "queue"),
                      (30, 36, 41, 29, 38)))
    log("platform", f"environments running now: {faas.running()}")

    section("Schedule trigger + a function that exceeds its timeout")
    faas.invoke("hourly_report", {}, "cron")
    faas.invoke("buggy", {})

    section("Idle period -> scale to zero")
    time.sleep(IDLE_TIMEOUT + 0.6)
    log("platform", f"environments running now: {faas.running()}")

    section("Next request after scale-to-zero pays a cold start again, and the global counter is RESET")
    faas.invoke("to_fahrenheit", {"city": "oslo", "celsius": 9.0})

    section("Billing: you pay only for execution time x memory")
    log("billing", f"total = {faas.bill_gb_s:.4f} GB-s ≈ ${faas.bill_gb_s * PRICE_PER_GB_S:.8f} "
                   f"(idle time costs nothing)")
    faas.shutdown()
    takeaway(
        "No servers to manage: deploy a function, the platform provisions environments per event.",
        "Cold starts add latency (runtime boot + imports); warm environments are reused.",
        "Autoscaling is per event and scale-to-zero makes idle cost 0 - pay-per-use (GB-s).",
        "Functions must be stateless: globals survive only by accident (warm env). Use external storage.",
        "Timeouts and limits are enforced by the platform; long work belongs in workflows (see 21).",
    )


if __name__ == "__main__":
    main()
