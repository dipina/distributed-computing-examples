"""22 · Container orchestration: declarative desired state + reconciliation loops (Kubernetes-style)  [NEW].

How do today's clouds run microservices (example 15) reliably? Not with scripts
that say *what to do*, but with **declarative desired state** ("I want 3
replicas of weather-api:v1") and **controllers** that run a **reconciliation
loop** forever:

    observe actual state  ->  diff with desired state  ->  act to converge  ->  repeat

This single pattern gives **self-healing** (a pod dies -> it is recreated),
**scaling** (change a number), **rolling updates** with zero downtime, and a
**Service** that load-balances only over *ready* pods.

Pods here are real OS processes running a tiny HTTP server.

Teaching guide: README.md in this folder (diagram, walkthrough, code map, questions).

Install / tools:
    (nothing: Python standard library only)
    Try it on real Kubernetes: kubectl https://kubernetes.io/docs/tasks/tools/
      kind https://kind.sigs.k8s.io/docs/user/quick-start/  (go install sigs.k8s.io/kind@latest | brew install kind)
      kind create cluster
      kubectl create deployment weather-api --image=nginx:1.27 --replicas=3
      kubectl delete pod <one-pod>      # watch it being recreated: kubectl get pods -w
      kubectl set image deployment/weather-api nginx=nginx:1.28 && kubectl rollout status deployment/weather-api

Tutorials & references:
    - Kubernetes: controllers (control loops)
      https://kubernetes.io/docs/concepts/architecture/controller/
    - Kubernetes: Deployments
      https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
    - Kubernetes basics: performing a rolling update
      https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/
    - Liveness, readiness and startup probes
      https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/
    - Burns et al., Borg, Omega, and Kubernetes (ACM Queue 2016)
      https://queue.acm.org/detail.cfm?id=2898444
    - Kopf: Kubernetes operators in Python
      https://kopf.readthedocs.io/

Run:  python examples/22_orchestration_reconciliation/demo.py
"""

from __future__ import annotations

import itertools
import json
import multiprocessing as mp
import socket
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.utils import banner, free_port, log, section, takeaway  # noqa: E402


def pod_main(name: str, version: str, port: int) -> None:
    """The 'container': a weather API returning who served the request."""
    time.sleep(0.3)  # start-up time (pull image, boot app...)

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            body = json.dumps({"pod": name, "version": version}).encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a: Any) -> None:
            pass

    HTTPServer(("127.0.0.1", port), H).serve_forever()


@dataclass
class Pod:
    """Actual state of one pod."""

    name: str
    version: str
    port: int
    proc: mp.Process

    def ready(self) -> bool:
        """Readiness probe: does it accept connections?"""
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=0.05):
                return True
        except OSError:
            return False


class Cluster:
    """API server (desired state store) + deployment controller + service."""

    def __init__(self) -> None:
        """Empty cluster; start the control loop."""
        self.desired: dict[str, dict[str, Any]] = {}  # the 'etcd' content
        self.pods: dict[str, list[Pod]] = {}
        self.lock = threading.Lock()
        self.ids = itertools.count(1)
        self.rr = itertools.count()
        self._stop = threading.Event()
        threading.Thread(target=self._control_loop, daemon=True).start()

    # ------------------------------------------------ "kubectl apply"
    def apply(self, name: str, image_version: str, replicas: int) -> None:
        """Declare the desired state. Nothing is started here!"""
        with self.lock:
            self.desired[name] = {"version": image_version, "replicas": replicas}
        log("kubectl", f"apply deployment/{name}: {image_version} x{replicas}")

    # ------------------------------------------------ controller
    def _control_loop(self) -> None:
        while not self._stop.wait(0.15):
            with self.lock:
                for name, spec in self.desired.items():
                    self._reconcile(name, spec)

    def _start_pod(self, name: str, version: str) -> None:
        pod_name, port = f"{name}-{version}-{next(self.ids)}", free_port()
        p = mp.Process(target=pod_main, args=(pod_name, version, port), daemon=True)
        p.start()
        self.pods.setdefault(name, []).append(Pod(pod_name, version, port, p))
        log("controller", f"+ create pod {pod_name}")

    def _delete_pod(self, name: str, pod: Pod, why: str) -> None:
        pod.proc.terminate()
        self.pods[name].remove(pod)
        log("controller", f"- delete pod {pod.name} ({why})")

    # See: https://kubernetes.io/docs/concepts/architecture/controller/
    def _reconcile(self, name: str, spec: dict[str, Any]) -> None:
        pods = self.pods.setdefault(name, [])
        for pod in [p for p in pods if not p.proc.is_alive()]:  # self-healing
            pods.remove(pod)
            log("controller", f"! observed pod {pod.name} is dead -> will replace")
        want_v, want_n = spec["version"], spec["replicas"]
        new = [p for p in pods if p.version == want_v]
        old = [p for p in pods if p.version != want_v]
        if old:  # rolling update: surge 1, maxUnavailable 0
            if len(pods) <= want_n:
                self._start_pod(name, want_v)
            elif all(p.ready() for p in new):
                self._delete_pod(name, old[0], f"rolling update to {want_v}")
            return
        if len(pods) < want_n:
            for _ in range(want_n - len(pods)):
                self._start_pod(name, want_v)
        elif len(pods) > want_n:
            for pod in pods[want_n:]:
                self._delete_pod(name, pod, "scale down")

    # ------------------------------------------------ Service (load balancer)
    def request(self, name: str) -> dict[str, Any] | None:
        """Round-robin over READY pods only (like a ClusterIP Service)."""
        with self.lock:
            ready = [p for p in self.pods.get(name, []) if p.ready()]
        if not ready:
            return None
        pod = ready[next(self.rr) % len(ready)]
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{pod.port}/", timeout=1) as r:
                return json.loads(r.read())
        except OSError:
            return None

    def get_pods(self, name: str) -> str:
        """``kubectl get pods`` output."""
        with self.lock:
            return ", ".join(f"{p.name}({'Ready' if p.ready() else 'Starting'})" for p in self.pods.get(name, []))

    def wait_converged(self, name: str, timeout: float = 10) -> None:
        """Block until actual == desired and all pods are ready."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with self.lock:
                spec, pods = self.desired[name], self.pods.get(name, [])
                ok = len(pods) == spec["replicas"] and all(p.version == spec["version"] and p.ready() for p in pods)
            if ok:
                log("kubectl", f"get pods -> {self.get_pods(name)}")
                return
            time.sleep(0.05)
        raise TimeoutError("did not converge")

    def shutdown(self) -> None:
        """Stop the controller and all pods."""
        self._stop.set()
        with self.lock:
            for pods in self.pods.values():
                for p in pods:
                    p.proc.terminate()


def main() -> None:
    """Run the orchestration scenarios."""
    banner("22 · Orchestration: desired state + reconciliation loop (Kubernetes-style)", added=True)
    k8s = Cluster()

    section("Declare: 3 replicas of weather-api:v1 (we never start processes ourselves)")
    k8s.apply("weather-api", "v1", 3)
    k8s.wait_converged("weather-api")
    served = [k8s.request("weather-api") for _ in range(6)]
    log("service", f"6 requests load-balanced over pods: {[s['pod'] for s in served if s]}")

    section("Chaos: a pod is killed -> the controller notices the drift and heals it")
    with k8s.lock:
        victim = k8s.pods["weather-api"][0]
    victim.proc.kill()
    log("chaos", f"💥 killed {victim.name}")
    time.sleep(0.2)
    k8s.wait_converged("weather-api")

    section("Scale: just change the number in the desired state")
    k8s.apply("weather-api", "v1", 5)
    k8s.wait_converged("weather-api")
    k8s.apply("weather-api", "v1", 2)
    k8s.wait_converged("weather-api")

    section("Rolling update v1 -> v2 while traffic keeps flowing (zero downtime)")
    k8s.apply("weather-api", "v2", 2)
    versions, failures, after_done, end = [], 0, 0, time.monotonic() + 8
    while time.monotonic() < end:
        r = k8s.request("weather-api")
        if r is None:
            failures += 1
        else:
            versions.append(r["version"])
        with k8s.lock:
            done = all(p.version == "v2" for p in k8s.pods["weather-api"]) and len(k8s.pods["weather-api"]) == 2
        after_done += done
        if after_done >= 6:  # a few more requests once the rollout is complete
            break
        time.sleep(0.03)
    k8s.wait_converged("weather-api")
    timeline = "".join(v[1] for v in versions)
    log("service", f"{len(versions)} requests during rollout, failed={failures}")
    log("service", f"version that answered each request, in order: {timeline}")
    k8s.shutdown()

    takeaway(
        "Declarative: you state WHAT you want; controllers work out HOW (and keep doing it).",
        "Level-triggered reconciliation makes the system self-healing: any drift is corrected.",
        "Rolling updates + readiness probes give zero-downtime deployments.",
        "This is how Kubernetes (and GitOps tools like Argo CD/Flux) run the microservices of example 15.",
    )


if __name__ == "__main__":
    main()
