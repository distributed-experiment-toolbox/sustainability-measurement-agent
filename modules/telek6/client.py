"""HTTP client for a telek6 server (https://github.com/distributed-experiment-toolbox/telek6).

Starts workloads driven by k6 scripts, similar to telelocust. Every call that
acts on a run takes its token explicitly: the client holds no "current run",
so a late call from one run can never act on another.
"""
import base64
import io
import logging
import threading
import zipfile
from typing import Dict, List, Optional

import requests

REQUEST_TIMEOUT = 10
POLL_INTERVAL_S = 1
POLL_RETRIES = 5

log = logging.getLogger(__name__)


class Telek6Error(requests.HTTPError):
    """A telek6 request that failed, with the server's own error body.

    A requests.HTTPError, so wait() still retries it like any other transient
    request error."""


def _b64(path: str) -> str:
    with open(path, "rb") as fh:
        return base64.b64encode(fh.read()).decode("ascii")


def _check(r: requests.Response, what: str) -> requests.Response:
    if r.status_code >= 400:
        try:
            body = r.json().get("error", r.text)
        except ValueError:
            body = r.text
        raise Telek6Error(f"telek6 {what}: HTTP {r.status_code}: {body}", response=r)
    return r


class Telek6Client:
    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def health_check(self) -> bool:
        r = requests.get(f"{self.url}/healthz", timeout=REQUEST_TIMEOUT)
        return r.status_code == 200

    def start_run(self, script_path: str, files: Optional[Dict[str, str]] = None,
                  env: Optional[Dict[str, str]] = None, args: Optional[List[str]] = None,
                  timeout_s: Optional[int] = None, paused: bool = False) -> str:
        """Start a k6 run; returns its token.

        paused: k6 initialises every VU and waits for resume(); timeout_s then
        counts from the resume. Needs telek6 >= 0.3.0."""
        body = {"script_base64": _b64(script_path),
                "files": {name: _b64(path) for name, path in (files or {}).items()},
                "env": env or {}, "args": args or [], "timeout_s": timeout_s}
        if paused:
            body["paused"] = True
        r = _check(requests.post(f"{self.url}/runs/start", json=body, timeout=REQUEST_TIMEOUT), "start")
        token: str = r.json()["token"]
        return token

    def status(self, token: str) -> dict:
        r = _check(requests.get(f"{self.url}/runs/{token}", timeout=REQUEST_TIMEOUT), f"status of run {token}")
        return r.json()

    def resume(self, token: str) -> float:
        """Resume a ready paused run; returns the server's resumed_at (epoch s)."""
        r = _check(requests.post(f"{self.url}/runs/{token}/resume", json={}, timeout=REQUEST_TIMEOUT),
                   f"resume of run {token}")
        resumed_at: float = r.json()["resumed_at"]
        return resumed_at

    def stop(self, token: str) -> dict:
        """Stop a run gracefully: k6 still writes its summary."""
        r = _check(requests.post(f"{self.url}/runs/{token}/stop", json={}, timeout=REQUEST_TIMEOUT + 15),
                   f"stop of run {token}")
        return r.json()

    def reset(self) -> List[str]:
        """SIGKILL every k6 the server still runs; returns their tokens. Their run
        directories stay downloadable."""
        r = _check(requests.post(f"{self.url}/runs/reset", json={}, timeout=REQUEST_TIMEOUT + 15), "reset")
        killed: List[str] = r.json()["killed"]
        return killed

    def wait(self, token: str, cancel: threading.Event,
             poll_interval_s: float = POLL_INTERVAL_S) -> Optional[dict]:
        """Poll until finished or `cancel`; a few transient errors are retried, more raise.

        Returns the final status, or None when cancelled (the run was stopped).
        """
        retries = 0
        while not cancel.is_set():
            try:
                st = self.status(token)
                retries = 0
            except requests.RequestException as exc:
                retries += 1
                if retries > POLL_RETRIES:
                    try:   # best effort: never leave k6 loading the SUT behind an SMA that moved on
                        self.stop(token)
                    except requests.RequestException as stop_exc:
                        log.error("telek6 run %s could not be stopped: %s", token, stop_exc)
                    raise
                log.warning("telek6 poll failed (%d/%d): %s", retries, POLL_RETRIES, exc)
                cancel.wait(retries * poll_interval_s)
                continue
            if st["status"] != "running":
                return st
            cancel.wait(poll_interval_s)
        log.info("cancel set; stopping telek6 run %s", token)
        self.stop(token)
        return None

    def download(self, token: str, into_dir: str) -> List[str]:
        r = _check(requests.get(f"{self.url}/runs/{token}/download", timeout=REQUEST_TIMEOUT * 6),
                   f"download of run {token}")
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(into_dir)
            return zf.namelist()
