"""20 · Actor model & distributed tasks/futures (Ray-style)  [NEW].

Modern distributed computing frameworks for AI/ML and data (Ray, Dask, Akka,
Orleans, Erlang/Elixir, Dapr actors) offer two simple primitives:

* **Remote tasks** - stateless functions executed somewhere in the cluster.
  ``f.remote(x)`` returns a **future** (ObjectRef) immediately; ``get(ref)`` waits.
  Futures can be passed to other tasks -> a **dataflow graph** is built for you.
* **Actors** - stateful objects living in their own process. They communicate
  ONLY by asynchronous messages that queue in a **mailbox** and are processed
  one at a time -> no shared memory, no locks.
* **Supervision** ("let it crash", Erlang/Akka): a supervisor restarts failed actors.

We implement a mini runtime on top of ``multiprocessing`` with the same API as
Ray. If Ray is installed (``pip install ray``) a final part runs the same code
on the real thing.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install "ray[default]"  # optional bonus part (large download)
    ray start --head            # start a real multi-node cluster head (then: ray stop)

Tutorials & references:
    - What's Ray Core? (tasks, actors, objects)
      https://docs.ray.io/en/latest/ray-core/walkthrough.html
    - Installing Ray
      https://docs.ray.io/en/latest/ray-overview/installation.html
    - Ray actors
      https://docs.ray.io/en/latest/ray-core/actors.html
    - concurrent.futures (futures, process pools)
      https://docs.python.org/3/library/concurrent.futures.html
    - Erlang supervision principles ('let it crash')
      https://www.erlang.org/doc/system/sup_princ.html
    - Pykka: actor model for Python
      https://pykka.readthedocs.io/
    - Akka
      https://akka.io/

Run:  python examples/20_actor_model/demo.py
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import sys
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import INITIAL_READINGS  # noqa: E402
from common.utils import banner, log, section, takeaway  # noqa: E402


# ================================================================= mini runtime
class ObjectRef:
    """A future for a value that may live in another process."""

    def __init__(self, fut: Future[Any]) -> None:
        """Wrap a concurrent.futures Future."""
        self._fut = fut


def get(refs: ObjectRef | list[ObjectRef], timeout: float | None = None) -> Any:
    """Block until the value(s) are ready (like ``ray.get``)."""
    if isinstance(refs, list):
        return [r._fut.result(timeout) for r in refs]
    return refs._fut.result(timeout)


class Runtime:
    """Holds the worker pool used for remote tasks."""

    pool: ProcessPoolExecutor | None = None

    @classmethod
    def init(cls, num_workers: int) -> None:
        """Start the worker processes (like ``ray.init``)."""
        cls.pool = ProcessPoolExecutor(num_workers)

    @classmethod
    def shutdown(cls) -> None:
        """Stop the workers."""
        if cls.pool:
            cls.pool.shutdown()


class RemoteFunction:
    """Result of decorating a function with :func:`remote`."""

    def __init__(self, fn: Callable[..., Any]) -> None:
        """Keep the function (it must be importable at module level)."""
        self.fn = fn

    def remote(self, *args: Any) -> ObjectRef:
        """Schedule the task; ObjectRef arguments are resolved first (dataflow)."""
        out: Future[Any] = Future()

        def resolve_and_submit() -> None:
            real = [get(a) if isinstance(a, ObjectRef) else a for a in args]
            assert Runtime.pool is not None
            inner = Runtime.pool.submit(self.fn, *real)
            inner.add_done_callback(lambda f: out.set_exception(f.exception()) if f.exception() else out.set_result(f.result()))

        threading.Thread(target=resolve_and_submit, daemon=True).start()
        return ObjectRef(out)


def _actor_loop(cls: type, args: tuple[Any, ...], inbox: "mp.Queue[Any]", outbox: "mp.Queue[Any]") -> None:
    """Process hosting one actor: handle mailbox messages sequentially."""
    instance = cls(*args)
    while (msg := inbox.get()) is not None:
        call_id, method, margs = msg
        try:
            outbox.put((call_id, True, getattr(instance, method)(*margs)))
        except Exception as exc:  # noqa: BLE001
            outbox.put((call_id, False, repr(exc)))


class ActorHandle:
    """Client-side handle; ``handle.method.remote(...)`` sends a message."""

    def __init__(self, cls: type, args: tuple[Any, ...], supervised: bool) -> None:
        """Spawn the actor process."""
        self._cls, self._args, self._supervised, self._killed = cls, args, supervised, False
        self._futures: dict[int, Future[Any]] = {}
        self._ids = iter(range(10**9))
        self._lock = threading.Lock()
        self._spawn()

    def _spawn(self) -> None:
        self._inbox: mp.Queue[Any] = mp.Queue()
        self._outbox: mp.Queue[Any] = mp.Queue()
        self._proc = mp.Process(target=_actor_loop, args=(self._cls, self._args, self._inbox, self._outbox), daemon=True)
        self._proc.start()
        threading.Thread(target=self._pump, args=(self._outbox, self._proc), daemon=True).start()

    def _pump(self, outbox: "mp.Queue[Any]", proc: mp.Process) -> None:
        while True:
            try:
                call_id, ok, value = outbox.get(timeout=0.1)
            except Exception:  # noqa: BLE001 - queue.Empty
                if not proc.is_alive():
                    self._on_crash(proc)
                    return
                continue
            fut = self._futures.pop(call_id)
            fut.set_result(value) if ok else fut.set_exception(RuntimeError(value))

    def _on_crash(self, proc: mp.Process) -> None:
        if self._killed:
            return
        with self._lock:
            for fut in self._futures.values():
                fut.set_exception(RuntimeError("actor died"))
            self._futures.clear()
            log("supervisor", f"actor {self._cls.__name__} (pid {proc.pid}) died with exit code {proc.exitcode}")
            if self._supervised:
                self._spawn()
                log("supervisor", f"restarted {self._cls.__name__} in new pid {self._proc.pid} (state reset!)")

    def __getattr__(self, method: str) -> Any:
        handle = self

        class _Method:
            def remote(self, *args: Any) -> ObjectRef:
                with handle._lock:
                    cid = next(handle._ids)
                    fut: Future[Any] = Future()
                    handle._futures[cid] = fut
                    handle._inbox.put((cid, method, args))
                return ObjectRef(fut)
        return _Method()

    def kill(self) -> None:
        """Stop the actor for good."""
        self._supervised, self._killed = False, True
        self._inbox.put(None)


def remote(obj: Any = None, *, supervised: bool = False) -> Any:
    """Decorator turning a function into a remote task or a class into an actor."""
    def wrap(o: Any) -> Any:
        if isinstance(o, type):
            class ActorClass:
                @staticmethod
                def remote(*args: Any) -> ActorHandle:
                    return ActorHandle(o, args, supervised)
            return ActorClass
        return RemoteFunction(o)
    return wrap(obj) if obj is not None else wrap


# ================================================================= user code
def climate_simulation(city: str, steps: int) -> dict[str, Any]:
    """CPU-heavy pure function (stateless task)."""
    acc = 0.0
    for i in range(steps):
        acc += math.sin(i) * math.cos(i / 3)
    return {"city": city, "pid": os.getpid(), "score": round(acc, 3)}


def combine(*results: dict[str, Any]) -> str:
    """Task that depends on the outputs of other tasks."""
    return f"combined {len(results)} simulations from pids {sorted({r['pid'] for r in results})}"


class StationAggregator:
    """Actor: owns mutable state; messages are processed one at a time."""

    def __init__(self, city: str) -> None:
        """Initial state."""
        self.city, self.readings = city, []  # type: ignore[var-annotated]

    def add(self, t: float) -> int:
        """Add a reading (no lock needed: the mailbox serialises calls)."""
        if t > 100:
            os._exit(3)  # simulate a fatal crash (segfault, OOM kill...) - no cleanup at all
        self.readings.append(t)
        return len(self.readings)

    def mean(self) -> float:
        """Average of the stored readings."""
        return round(sum(self.readings) / len(self.readings), 2) if self.readings else float("nan")


simulate = remote(climate_simulation)
combine_r = remote(combine)
Aggregator = remote(StationAggregator)
SupervisedAggregator = remote(supervised=True)(StationAggregator)
STEPS = 1_500_000


def mini_runtime_demo() -> None:
    """Tasks, dataflow, actors and supervision on the mini runtime."""
    workers = max(2, min(4, os.cpu_count() or 2))
    Runtime.init(workers)
    get(simulate.remote("warmup", 10))  # start the worker processes

    section(f"Remote tasks: sequential vs parallel on {workers} worker processes")
    t0 = time.perf_counter()
    for c in INITIAL_READINGS:
        climate_simulation(c, STEPS)
    seq = time.perf_counter() - t0
    t0 = time.perf_counter()
    refs = [simulate.remote(c, STEPS) for c in INITIAL_READINGS]  # returns instantly
    log("driver", f"got {len(refs)} ObjectRefs immediately, work runs in the background")
    results = get(refs)
    par = time.perf_counter() - t0
    log("driver", f"sequential {seq:.2f}s vs parallel {par:.2f}s -> speed-up x{seq / par:.1f}; pids={sorted({r['pid'] for r in results})}")

    section("Dataflow: pass futures to other tasks, the runtime resolves dependencies")
    refs = [simulate.remote(c, 200_000) for c in ("bilbao", "oslo")]
    log("driver", get(combine_r.remote(*refs)))

    section("Actors: stateful, one process each, mailbox => no locks even with concurrent callers")
    agg = Aggregator.remote("bilbao")
    refs = [agg.add.remote(t) for t in (17.5, 18.0, 19.2, 20.1)]
    log("driver", f"add() replies (in mailbox order): {get(refs)}; mean={get(agg.mean.remote())}")
    agg.kill()

    section("Supervision: 'let it crash' - a supervisor restarts the failed actor")
    sup = SupervisedAggregator.remote("oslo")
    get([sup.add.remote(t) for t in (8.0, 9.0)])
    bad = sup.add.remote(999.0)
    try:
        get(bad, timeout=5)
    except RuntimeError as exc:
        log("driver", f"call failed: {exc}")
    time.sleep(0.3)
    log("driver", f"actor alive again: add(7.5) -> {get(sup.add.remote(7.5), timeout=5)} reading(s); "
                  f"mean={get(sup.mean.remote())} (state was lost: persist it if it matters)")
    sup.kill()
    Runtime.shutdown()


# See: https://docs.ray.io/en/latest/ray-core/walkthrough.html
def real_ray_demo() -> None:
    """Same ideas on Ray, if installed."""
    section("Bonus: the same program on the real Ray runtime")
    try:
        import ray
    except ImportError:
        log("ray", "Ray not installed -> skipped (pip install ray). The API above mirrors Ray's.")
        return
    ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR", log_to_driver=False)
    sim = ray.remote(climate_simulation)
    Agg = ray.remote(StationAggregator)
    refs = [sim.remote(c, 300_000) for c in INITIAL_READINGS]
    log("ray", f"tasks ran in Ray worker pids -> {sorted({r['pid'] for r in ray.get(refs)})}")
    a = Agg.remote("madrid")
    ray.get([a.add.remote(t) for t in (24.1, 26.3)])
    log("ray", f"actor mean -> {ray.get(a.mean.remote())}")
    ray.shutdown()


def main() -> None:
    """Run the actor/task demos."""
    banner("20 · Actor model & distributed futures (Ray/Akka/Orleans style)", added=True)
    mini_runtime_demo()
    real_ray_demo()
    takeaway(
        "Tasks + futures: write sequential-looking code, get parallel/distributed execution and dataflow.",
        "Actors encapsulate state and process one message at a time: concurrency without locks.",
        "Failures are isolated per actor; supervisors restart them ('let it crash').",
        "Ray is how today's LLM training/serving and RL pipelines scale Python across clusters.",
    )


if __name__ == "__main__":
    main()
