"""01 · Message-passing paradigm (slide 4).

The most basic IPC style: processes share *nothing* and cooperate only by
``send``-ing and ``receive``-ing messages. Here we use three OS-level mechanisms:

1. A duplex **pipe** between two processes, used request/reply style.
2. A **queue** with several producer processes and one consumer.
3. **Shared memory** as a contrast: no messages, just a common memory region.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    pip install mpi4py          # optional: real message passing for HPC (needs an MPI runtime)

Tutorials & references:
    - multiprocessing (Process, Pipe, Queue)
      https://docs.python.org/3/library/multiprocessing.html
    - multiprocessing programming guidelines
      https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    - multiprocessing.shared_memory
      https://docs.python.org/3/library/multiprocessing.shared_memory.html
    - mpi4py: MPI for Python (message passing in HPC)
      https://mpi4py.readthedocs.io/

Run:  python examples/01_message_passing/demo.py
"""

from __future__ import annotations

import multiprocessing as mp
import os
import sys
from multiprocessing.connection import Connection
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.domain import WeatherService  # noqa: E402
from common.utils import banner, log, section, takeaway  # noqa: E402


def weather_process(conn: Connection) -> None:
    """Child process: receive requests over a pipe, reply with results.

    Message format (a tuple): ``(operation, *args)``; ``("stop",)`` ends it.
    """
    service = WeatherService()  # the state lives ONLY in this process
    me = f"child:{os.getpid()}"
    while True:
        op, *args = conn.recv()  # receive (blocking)
        log(me, f"received {op}{tuple(args)}")
        if op == "stop":
            conn.send("bye")
            break
        try:
            result = getattr(service, op)(*args)
            conn.send(("ok", result))  # send
        except Exception as exc:  # noqa: BLE001 - errors travel as messages, too
            conn.send(("error", repr(exc)))
    conn.close()


def sensor(city: str, temps: list[float], q: "mp.Queue[tuple[str, float] | None]") -> None:
    """Producer process: push readings into a queue, then a ``None`` sentinel."""
    for t in temps:
        q.put((city, t))
        log(f"sensor:{city}", f"put {t}°C")
    q.put(None)


def shm_writer(name: str) -> None:
    """Write bytes into an existing shared-memory block (no message is sent)."""
    shm = SharedMemory(name=name)
    shm.buf[:5] = b"HELLO"
    shm.close()


def main() -> None:
    """Run the three message-passing mini-demos."""
    banner("01 · Message passing: pipes, queues (and shared memory)", "4")

    section("1) Request/reply over a duplex Pipe (send / receive)")
    # See: https://docs.python.org/3/library/multiprocessing.html#exchanging-objects-between-processes
    parent_end, child_end = mp.Pipe(duplex=True)
    child = mp.Process(target=weather_process, args=(child_end,))
    child.start()  # 'connect'
    for msg in [("get_temperature", "bilbao"), ("report", "bilbao", 21.0),
                ("summary", "bilbao"), ("get_temperature", "atlantis")]:
        parent_end.send(msg)
        log("parent", f"sent {msg} -> got {parent_end.recv()}")
    parent_end.send(("stop",))
    log("parent", f"final reply: {parent_end.recv()}")
    child.join()  # 'disconnect'

    section("2) Many producers -> one consumer through a Queue (asynchronous)")
    q: "mp.Queue[tuple[str, float] | None]" = mp.Queue()
    producers = [mp.Process(target=sensor, args=(c, t, q))
                 for c, t in {"bilbao": [18.1, 18.4], "oslo": [7.0, 6.5, 6.9]}.items()]
    for p in producers:
        p.start()
    done, received = 0, []
    while done < len(producers):
        item = q.get()
        if item is None:
            done += 1
        else:
            received.append(item)
    for p in producers:
        p.join()
    log("consumer", f"collected {len(received)} readings (order is not guaranteed!): {received}")

    section("3) Contrast: shared memory (processes share state, no messages)")
    # See: https://docs.python.org/3/library/multiprocessing.shared_memory.html
    shm = SharedMemory(create=True, size=16)
    try:
        w = mp.Process(target=shm_writer, args=(shm.name,))
        w.start()
        w.join()
        log("parent", f"read from shared memory: {bytes(shm.buf[:5])!r}")
    finally:
        shm.close()
        shm.unlink()

    takeaway(
        "Processes do not share variables: the WeatherService state lives in the child only.",
        "The whole protocol is 'send' + 'receive' of self-describing messages (here tuples).",
        "Queues decouple producers from consumers; arrival order across producers is not guaranteed.",
        "Every later paradigm (sockets, RPC, REST, brokers...) is built on top of message passing.",
    )


if __name__ == "__main__":
    main()
