"""Telek6 SMA module: drives a long-lived telek6 pod as the treatment.

    modules:
      telek6:
        module: telek6
        config:
          url: http://localhost:30124   # the telek6 Service; derived when port_forward is set
          timeout_s: 900                # per-run cap on the server side, overridable per run
          poll_interval_s: 1
          # When k6 starts (see "Prepared runs" below): cold | treatment | left_window
          start_at: cold
          prepare_timeout_s: 120        # prepared runs: how long k6 may take to initialise every VU
          # The workload. Every key can also be given per run (agent.run(**kwargs));
          # a run argument wins over the config, None counts as not given, and
          # files/env/args are replaced whole, not merged. Not allowed with
          # start_at: left_window, where the workload must come from the run.
          script_path: null
          files: null                   # {name in the run dir: local path}
          env: null                     # {NAME: value}
          args: null                    # extra `k6 run` arguments, e.g. ["--vus", "10"]
          download_dir: null            # where run data goes; a temp dir moved under the report when null
          # Deployment options, as in telelocust. deploy: true applies the bundled
          # telek6.yaml, a string is a path to your own manifest; both get the patches
          # below. apply is idempotent, so a long-lived pod survives sessions;
          # undeploy: true deletes it at teardown.
          deploy: false
          namespace: telek6
          image: null                   # default: the manifest's pinned telek6 tag
          imagePullSecrets: null        # e.g. myregistrykey
          nodeSelector: null            # e.g. ["kubernetes.io/arch=amd64", "node-role.kubernetes.io/control-plane="]
          deploy_timeout_s: 300
          undeploy: false
          # Networking options, as in telelocust: disabled (use url) | proxy (kubectl
          # port-forward to localhost:5124) | nodeport (nodeIP + the Service's nodePort)
          port_forward: disabled
          nodeIP: null                  # nodeport only; default: the host of the kubeconfig server
          kube_context: null            # ambient KUBECONFIG/current-context when null
    observation:
      mode: module
      module_trigger: telek6

Terms. A *run* is one SMA run (onRunStart ... onRunEnd). A *k6 run* is one
`k6 run` process on the telek6 server, named by its *token*. *Prepare* starts a
k6 run paused: k6 parses the script, initialises every VU and waits. *Resume*
lets a prepared k6 run start its load.

`trigger(cancel, **kwargs)` takes the workload keys above. It waits for the k6
run, stops it on cancel, and downloads its directory (script, files,
console.ndjson, stdout.ndjson with the summary, stderr.log). Returned meta:
token, k6_status (complete|failed|cancelled|timeout), exit_code, started_at,
finished_at, resumed_at, resume_requested_at, prepare_s, start_at, stopped,
run_dir, downloaded_files. resumed_at is the telek6 pod's clock;
resume_requested_at is this process's.

Prepared runs keep k6's own start-up (script parse, VU initialisation) out of
the treatment. Needs telek6 >= 0.3.0.

- start_at: cold (default): k6 starts inside the trigger, as before; its
  start-up falls inside the treatment.
- start_at: treatment: the module prepares the k6 run in its onRunStart and
  resumes it inside the trigger. A heavy k6 `setup()` still runs after the
  resume, so inside the treatment.
- start_at: left_window: prepared the same way, resumed in the module's
  onLeftWindowStart, so the left window is the start of the same k6 run. Write
  a warm-up as a k6 scenario at startTime 0 and the measured load as a second
  scenario at startTime = observation.window.left; k6 counts startTime from the
  resume. The module cannot see the window, so this alignment is yours. Needs
  observation.window; without one the run fails with status "error".

The per-run workload reaches the module through SMA's onRunParameters hook,
fired just before onRunStart. The module prepares in its own onRunStart, so an
observer that must finish before k6 initialises (a SUT reset, an idle gate)
has to be notified first: pass it to the SMA constructor. Preparing costs CPU on
the generator node within SMA's observed range, before the left window.

A failed prepare raises in onRunStart and aborts the run. A failed resume makes
the trigger raise, and SMA records the run with status "error". A direct
trigger() call outside an SMA run (nothing prepared) starts k6 cold.

One SMA client per telek6 pod: in prepared modes the module kills every k6 on
the server (`reset()`) at onSetup, at every onRunStart, at onRunEnd and at
teardown, never from the trigger. Killed runs stay downloadable while the
telek6 process lives; the module never relies on that.
"""
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from importlib import resources
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import requests
import yaml

from modules.telek6.client import Telek6Client
from sma.model import SMAObserver, Triggerable

log = logging.getLogger(__name__)

BUNDLED_MANIFEST = resources.files("modules").joinpath("telek6/telek6.yaml")
CONTAINER_PORT = 5124
LOCAL_PORT = 5124

START_AT = ("cold", "treatment", "left_window")
WORKLOAD = ("script_path", "files", "env", "args", "timeout_s", "download_dir")


class Telek6Forwarding(Enum):
    DISABLED = auto()
    PROXY = auto()
    NODEPORT = auto()


@dataclass
class Telek6Config:
    url: Optional[str] = None
    timeout_s: Optional[int] = 900
    poll_interval_s: float = 1.0

    # when k6 starts, and the workload
    start_at: str = "cold"
    prepare_timeout_s: float = 120
    script_path: Optional[str] = None
    files: Optional[Dict[str, str]] = None
    env: Optional[Dict[str, str]] = None
    args: Optional[List[str]] = None
    download_dir: Optional[str] = None

    # deployment options
    deploy: Union[bool, str] = False   # true: bundled manifest; str: path to a manifest
    namespace: str = "telek6"
    image: Optional[str] = None
    imagePullSecrets: Optional[str] = None
    nodeSelector: Optional[List[str]] = None
    deploy_timeout_s: int = 300
    undeploy: bool = False

    # networking options
    port_forward: Telek6Forwarding = Telek6Forwarding.DISABLED
    nodeIP: Optional[str] = None
    kube_context: Optional[str] = None

    @staticmethod
    def from_dict(d: dict) -> "Telek6Config":
        unknown = set(d) - set(Telek6Config.__dataclass_fields__)
        if unknown:
            raise ValueError(f"telek6 config: unknown keys {sorted(unknown)}")
        pf = d.get("port_forward", Telek6Forwarding.DISABLED)
        if isinstance(pf, bool):
            pf = Telek6Forwarding.PROXY if pf else Telek6Forwarding.DISABLED
        elif isinstance(pf, str):
            pf = Telek6Forwarding[pf.upper()]
        d["port_forward"] = pf
        c = Telek6Config(**d)
        if pf == Telek6Forwarding.DISABLED and not c.url:
            raise ValueError("telek6 config: url is required unless port_forward is set")
        if isinstance(c.deploy, str) and not os.path.isfile(c.deploy):
            raise ValueError(f"telek6 config: deploy manifest {c.deploy} does not exist")
        if c.start_at not in START_AT:
            raise ValueError(f"telek6 config: start_at must be one of {START_AT}, got {c.start_at!r}")
        if c.start_at == "left_window":
            set_here = [k for k in ("script_path", "files", "env", "args") if getattr(c, k) is not None]
            if set_here:
                # the module also observes runs another trigger owns; a config workload
                # would be resumed into those at their left window
                raise ValueError(f"telek6 config: start_at: left_window takes the workload from the run "
                                 f"arguments only; remove {set_here} from the config")
        if c.start_at != "cold" and not c.timeout_s:
            raise ValueError("telek6 config: prepared runs need a timeout_s, so a resumed run has a server-side cap")
        return c


@dataclass
class _Run:
    """One k6 run as this module knows it. The trigger captures the object at
    entry and writes only to it, so a trigger thread SMA has given up on can
    never touch the next run's state."""
    workload: Dict[str, Any]
    token: Optional[str] = None
    prepared: bool = False
    claimed: bool = False
    prepare_s: Optional[float] = None
    resume_requested_at: Optional[float] = None
    resumed_at: Optional[float] = None
    resume_error: Optional[str] = None
    download_dir: Optional[str] = None
    temp_dir: bool = False
    done: bool = False   # the trigger finished its download attempt


class Telek6SmaModule(SMAObserver, Triggerable):
    def __init__(self, config: dict):
        self.config = Telek6Config.from_dict(dict(config))
        self.client = Telek6Client(self.config.url or "")   # replaced in onSetup when the URL is derived
        self.manifest: Optional[str] = None                  # the patched manifest that was applied
        self.port_forward_process: Optional[subprocess.Popen] = None
        self._params: Dict[str, Any] = {}        # this run's arguments, from onRunParameters
        self._run: Optional[_Run] = None         # the k6 run this SMA run prepared
        self._downloads: List[_Run] = []         # temp download dirs to move under the report

    @property
    def prepared_mode(self) -> bool:
        return self.config.start_at != "cold"

    # ── deployment (kubectl; a Kubernetes API client is the upgrade if kubectl is ever not on PATH)
    def _kubectl(self, *args: str, check: bool = True) -> str:
        cmd = ["kubectl"] + (["--context", self.config.kube_context] if self.config.kube_context else []) + list(args)
        log.debug("kubectl: %s", " ".join(cmd))
        p = subprocess.run(cmd, capture_output=True, text=True)
        if check and p.returncode != 0:
            raise RuntimeError(f"kubectl {' '.join(args)} failed ({p.returncode}): {p.stderr.strip()}")
        return p.stdout

    def prepare_manifest(self) -> str:
        """Load the bundled or configured manifest, apply the config's patches, write it to a temp file."""
        src = BUNDLED_MANIFEST if self.config.deploy is True else str(self.config.deploy)
        with open(str(src)) as fh:
            docs = list(yaml.safe_load_all(fh))
        for doc in docs:
            if doc.get("kind") != "Deployment":
                continue
            pod = doc["spec"]["template"]["spec"]
            if self.config.nodeSelector:
                pod["nodeSelector"] = dict(s.split("=", 1) for s in self.config.nodeSelector)
            if self.config.image:
                for c in pod["containers"]:
                    if c["name"] == "telek6":
                        c["image"] = self.config.image
            if self.config.imagePullSecrets:
                pod["imagePullSecrets"] = [{"name": self.config.imagePullSecrets}]
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", prefix="telek6-", delete=False) as tmp:
            yaml.safe_dump_all(docs, tmp)
        log.debug("patched telek6 manifest written to %s", tmp.name)
        return tmp.name

    def deploy(self) -> None:
        assert self.config.deploy, "deploy() needs config.deploy"
        ns = self.config.namespace
        self._kubectl("create", "namespace", ns, check=False)   # exists already on the second session
        self.manifest = self.prepare_manifest()
        log.info("applying telek6 manifest %s to namespace %s", self.manifest, ns)
        self._kubectl("-n", ns, "apply", "-f", self.manifest)
        self._kubectl("-n", ns, "wait", "deployment/telek6",
                      "--for=condition=Available", f"--timeout={self.config.deploy_timeout_s}s")

    def undeploy(self) -> None:
        manifest = self.manifest or self.prepare_manifest()
        log.info("deleting telek6 manifest %s", manifest)
        self._kubectl("-n", self.config.namespace, "delete", "-f", manifest, "--ignore-not-found")

    # ── networking
    def nodeport_url(self) -> str:
        port = self._kubectl("-n", self.config.namespace, "get", "svc", "telek6",
                             "-o", "jsonpath={.spec.ports[0].nodePort}").strip()
        host = self.config.nodeIP
        if not host:
            log.warning("deriving the node IP from the kubeconfig server; set nodeIP if that is not a cluster node")
            server = self._kubectl("config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}").strip()
            host = urlparse(server).hostname
        return f"http://{host}:{port}"

    def port_forward_url(self) -> str:
        cmd = ["kubectl"] + (["--context", self.config.kube_context] if self.config.kube_context else [])
        cmd += ["-n", self.config.namespace, "port-forward", "deployment/telek6", f"{LOCAL_PORT}:{CONTAINER_PORT}"]
        self.port_forward_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert self.port_forward_process.stdout is not None
        first = self.port_forward_process.stdout.readline()   # "Forwarding from ..." once the tunnel is up
        if self.port_forward_process.poll() is not None:
            raise RuntimeError(f"kubectl port-forward exited: {first.strip()}")
        log.info("port-forward established: %s", first.strip())
        return f"http://localhost:{LOCAL_PORT}"

    # ── server state
    def reset(self) -> List[str]:
        """Kill every k6 the telek6 server still runs; returns their tokens. Safe
        because a telek6 pod serves one SMA client. Their data stays downloadable."""
        killed = self.client.reset()
        if killed:
            log.warning("telek6 reset killed k6 runs %s", killed)
        return killed

    def _reset_quietly(self, when: str) -> None:
        try:
            self.reset()
        except Exception as exc:   # cleanup must never mask why a run ended
            log.error("telek6 reset %s failed: %s", when, exc)

    def _stop_quietly(self, token: str) -> None:
        try:
            self.client.stop(token)
        except Exception as exc:
            log.error("telek6 stop of run %s failed: %s", token, exc)

    def _workload(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Run arguments over the config, per key; None counts as not given."""
        return {k: kwargs[k] if kwargs.get(k) is not None else getattr(self.config, k) for k in WORKLOAD}

    # ── hooks
    def onSetup(self) -> None:
        if self.config.deploy:
            self.deploy()
        url = self.config.url
        if self.config.port_forward == Telek6Forwarding.PROXY:
            url = self.port_forward_url()
        elif self.config.port_forward == Telek6Forwarding.NODEPORT:
            url = self.nodeport_url()
        assert url, "telek6 URL not found"
        log.info("telek6 URL: %s", url)
        self.client = Telek6Client(url)
        for attempt in range(10):   # a just-deployed Service can lag its pod's readiness by a moment
            try:
                if self.client.health_check():
                    log.info("telek6 at %s is healthy", url)
                    break
            except requests.RequestException as exc:
                log.warning("telek6 health check %d/10 failed: %s", attempt + 1, exc)
            time.sleep(1)
        else:
            raise RuntimeError(f"telek6 at {url} is not healthy")
        if self.prepared_mode:
            self.reset()   # whatever a crashed or earlier session left behind

    def onRunParameters(self, **kwargs) -> None:
        self._params = dict(kwargs)   # only this: a raise here would leave the last run's parameters

    def onRunStart(self) -> None:
        if not self.prepared_mode:
            return
        self._run = None
        params, self._params = self._params, {}
        # Unconditionally, also on runs that prepare nothing: a run aborted after an
        # earlier prepare fires no onRunEnd, and its paused k6 must not idle into this one.
        self.reset()
        workload = self._workload(params)
        if not workload["script_path"]:
            log.info("telek6: no script_path for this run; nothing prepared")
            return
        if not workload["timeout_s"] or workload["timeout_s"] <= 0:
            raise ValueError("telek6: a prepared run needs a positive timeout_s")
        run = _Run(workload=workload)
        t0 = time.time()
        try:
            run.token = self.client.start_run(workload["script_path"], files=workload["files"], env=workload["env"],
                                              args=workload["args"], timeout_s=workload["timeout_s"], paused=True)
            st = self.client.status(run.token)
            if st.get("paused") is not True:
                raise RuntimeError(f"telek6 started run {run.token} unpaused: prepared runs need telek6 >= 0.3.0 "
                                   f"(server status: {st})")
            while not st.get("ready"):
                if st["status"] != "running":
                    raise RuntimeError(f"telek6 run {run.token} ended before it was ready (exit_code="
                                       f"{st.get('exit_code')}, stopped={st.get('stopped')}); see its stderr.log")
                if time.time() - t0 > self.config.prepare_timeout_s:
                    raise RuntimeError(f"telek6 run {run.token} not ready after {self.config.prepare_timeout_s}s")
                time.sleep(self.config.poll_interval_s)
                st = self.client.status(run.token)
        except BaseException:
            self._reset_quietly("after a failed prepare")   # also catches a start whose response was lost
            raise
        run.prepared, run.prepare_s = True, time.time() - t0
        self._run = run
        log.info("telek6 run %s prepared in %.2fs", run.token, run.prepare_s)

    def onLeftWindowStart(self) -> None:
        run = self._run
        if self.config.start_at != "left_window" or run is None or not run.prepared:
            return
        run.resume_requested_at = time.time()
        try:
            run.resumed_at = self.client.resume(run.token)
        except Exception as exc:
            run.resume_error = f"telek6 run {run.token}: resume at the left window failed: {exc}"
            self._stop_quietly(run.token)
            raise RuntimeError(run.resume_error) from exc   # SMA logs it; the trigger raises it again
        log.info("telek6 run %s resumed at the left window", run.token)

    def _claim(self, kwargs: Dict[str, Any]) -> Optional[_Run]:
        """The k6 run this SMA run prepared, if the trigger is its owner."""
        run = self._run
        if run is None or not run.prepared or run.claimed:
            return None
        if run.workload != self._workload(kwargs):
            # Not called by the run that prepared it (SMA passes the trigger the same
            # arguments as onRunParameters): leave nothing paused behind.
            log.warning("telek6 run %s was prepared for other arguments; stopping it and starting cold", run.token)
            self._stop_quietly(run.token)
            return None
        run.claimed = True
        return run

    def trigger(self, cancel: threading.Event, **kwargs) -> Dict[str, Any]:
        run = self._claim(kwargs)
        if run is None:
            start_at = "cold"
            workload = self._workload(kwargs)
            if not workload["script_path"]:
                raise ValueError("telek6 trigger: no script_path in the run arguments or the module config")
            run = _Run(workload=workload)
            t0 = time.time()
            run.token = self.client.start_run(workload["script_path"], files=workload["files"], env=workload["env"],
                                              args=workload["args"], timeout_s=workload["timeout_s"])
            log.info("telek6 run %s started (%.2fs after trigger)", run.token, time.time() - t0)
        else:
            start_at = self.config.start_at
            if start_at == "treatment":
                run.resume_requested_at = time.time()
                try:
                    run.resumed_at = self.client.resume(run.token)
                except Exception as exc:
                    self._stop_quietly(run.token)
                    raise RuntimeError(f"telek6 run {run.token}: resume at the treatment failed: {exc}") from exc
                log.info("telek6 run %s resumed", run.token)
            elif run.resume_error:
                raise RuntimeError(run.resume_error)   # already stopped in onLeftWindowStart
            elif run.resumed_at is None:
                self._stop_quietly(run.token)
                raise RuntimeError(f"telek6 run {run.token}: start_at is left_window, but onLeftWindowStart never "
                                   f"fired; is observation.window set?")
        run.download_dir = run.workload["download_dir"]
        if run.download_dir is None:
            run.download_dir, run.temp_dir = tempfile.mkdtemp(prefix="telek6-"), True
            self._downloads.append(run)
        token = run.token
        final = self.client.wait(token, cancel, self.config.poll_interval_s)
        try:
            files = self.client.download(token, run.download_dir)
        except Exception as exc:  # partial data is still evidence; the status says what happened
            log.error("telek6 download of run %s failed: %s", token, exc)
            files = []
        run.done = True
        st = self.client.status(token)   # re-fetch: the server sets stopped=timeout just after status=finished
        if final is None:
            status = "cancelled"
        elif st.get("stopped") == "timeout":
            status = "timeout"
        else:
            status = "complete" if st.get("exit_code") == 0 else "failed"
        return {"token": token, "k6_status": status, "exit_code": st.get("exit_code"),
                "started_at": st.get("started_at"), "finished_at": st.get("finished_at"),
                "resumed_at": st.get("resumed_at"), "resume_requested_at": run.resume_requested_at,
                "prepare_s": run.prepare_s, "start_at": start_at, "stopped": st.get("stopped"),
                "run_dir": run.download_dir, "downloaded_files": files}

    def onRunEnd(self, run: Any = None) -> None:
        if not self.prepared_mode:
            return
        mine, self._run, self._params = self._run, None, {}
        if mine is not None and mine.token:
            log.info("SMA run %s used telek6 run %s", getattr(run, "runHash", None), mine.token)
            self._stop_quietly(mine.token)   # graceful first, so k6 still writes its summary
        self._reset_quietly("after the run")

    def onReport(self, report: Any = None) -> None:
        for run in self._downloads:
            if not run.done:   # a trigger SMA gave up on; its data must not land under a later report
                log.warning("telek6 run %s is still downloading; its data stays in %s", run.token, run.download_dir)
                continue
            dest = os.path.join(report.location, "telek6", os.path.basename(run.download_dir))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(run.download_dir, dest)
            log.info("telek6 run data moved to %s", dest)
        self._downloads = []

    def onTeardown(self) -> None:
        if self.prepared_mode:
            self._reset_quietly("at teardown")   # before the port-forward, the route to the server, goes
        if self.port_forward_process is not None:
            self.port_forward_process.terminate()
        if self.config.deploy and self.config.undeploy:
            self.undeploy()
