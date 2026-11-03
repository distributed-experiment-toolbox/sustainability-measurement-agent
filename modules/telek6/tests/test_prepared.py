"""Self-check: the telek6 module's run lifecycle (start_at cold|treatment|left_window)
driven by SMA's real run loop, against an in-memory fake telek6 client.

python -m modules.telek6.tests.test_prepared
"""
import logging
import os
import sys
import tempfile
import threading
import time
from types import SimpleNamespace

from modules.telek6.main import WORKLOAD, Telek6SmaModule
from sma.model import SMASession
from sma.sma import SustainabilityMeasurementAgent

logging.basicConfig(level=logging.CRITICAL)


class FakeTelek6:
    """telek6's run table in memory. `calls` is shared with the Mark observers, so
    one list shows the order of client calls and SMA hooks."""

    def __init__(self, calls: list):
        self.calls, self.runs, self.n = calls, {}, 0
        self.resume_fails = False
        self.block_wait = False          # wait() blocks until cancel, then stops slowly
        self.off_main_resets = []        # resets issued from a thread other than SMA's

    def start_run(self, script_path, files=None, env=None, args=None, timeout_s=None, paused=False):
        self.n += 1
        t = f"t{self.n}"
        self.runs[t] = {"status": "running", "paused": paused, "ready": paused, "exit_code": None,
                        "resumed_at": None if paused else 1.0, "started_at": 0.0, "finished_at": None,
                        "stopped": None}
        self.calls.append(("start", t, "paused" if paused else "cold"))
        return t

    def status(self, t):
        return dict(self.runs[t])

    def resume(self, t):
        self.calls.append(("resume", t))
        if self.resume_fails:
            raise RuntimeError("telek6 resume: HTTP 502: k6 refused the resume")
        r = self.runs[t]
        assert r["ready"], f"resume of a run that is not ready: {r}"
        r.update(ready=False, resumed_at=2.0)
        return 2.0

    def _end(self, t, why, code=-9):
        r = self.runs[t]
        if r["status"] == "running":
            r.update(status="finished", exit_code=code, stopped=why, ready=False, finished_at=3.0)

    def stop(self, t):
        self.calls.append(("stop", t))
        self._end(t, "stop")
        return self.status(t)

    def reset(self):
        if threading.current_thread() is not threading.main_thread():
            self.off_main_resets.append(threading.current_thread().name)
        killed = [t for t, r in self.runs.items() if r["status"] == "running"]
        for t in killed:
            self._end(t, "reset")
        self.calls.append(("reset", tuple(killed)))
        return killed

    def wait(self, t, cancel, poll_interval_s=1):
        r = self.runs[t]
        if self.block_wait:
            cancel.wait()
            time.sleep(0.3)   # the server's graceful stop takes a while
            self.stop(t)
            return None
        if r["status"] == "running":
            assert not r["paused"] or r["resumed_at"] is not None, f"wait on a paused, never resumed run {t}"
            r.update(status="finished", exit_code=0, finished_at=3.0)
        return self.status(t)

    def download(self, t, into):
        self.calls.append(("download", t))
        os.makedirs(into, exist_ok=True)
        with open(os.path.join(into, "console.ndjson"), "w") as fh:
            fh.write("{}\n")
        return ["console.ndjson"]

    def running(self):
        return [t for t, r in self.runs.items() if r["status"] == "running"]


class Mark:
    """Writes the hooks it sees into the shared call list."""

    def __init__(self, calls, name, hooks=("onRunStart", "onLeftWindowStart", "onTreatmentStart")):
        for h in hooks:
            setattr(self, h, lambda h=h: calls.append(f"{name}.{h}"))


class Boom:
    def onRunStart(self):
        raise RuntimeError("preflight failed")


class IdleWait:
    def trigger(self, cancel, **kwargs):
        return {}


def harness(start_at, *, window=True, max_duration=None, **cfg):
    calls = []
    fake = FakeTelek6(calls)
    mod = Telek6SmaModule({"url": "http://telek6.invalid", "start_at": start_at, **cfg})
    mod.client = fake
    a = SustainabilityMeasurementAgent.__new__(SustainabilityMeasurementAgent)
    a.logger = logging.getLogger("t")
    a.config = SimpleNamespace(observation=SimpleNamespace(
        mode="module", module_trigger="telek6", max_duration=max_duration,
        window=SimpleNamespace(left=0, right=0) if window else None))
    a.modules = {"telek6": mod}
    a.observers = [Mark(calls, "caller"), mod]   # constructor observer first, then the module
    a.session = SMASession(name="t")
    runs = []

    def observe_once(rd, overwrite=False):   # no Prometheus: record the run, fire onReport
        runs.append(rd.run)
        a.notify_observers("onReport", report=SimpleNamespace(location=tempfile.mkdtemp()))
    a.observe_once = observe_once
    return a, mod, fake, calls, runs


def main() -> int:
    work = tempfile.mkdtemp()
    script = os.path.join(work, "s.js")
    open(script, "w").write("export default function () {}\n")
    dl = lambda: tempfile.mkdtemp(dir=work)

    # cold, the default: as before, no prepare and no resets
    a, mod, fake, calls, runs = harness("cold")
    a.run(script_path=script, download_dir=dl())
    assert [c for c in calls if isinstance(c, tuple)][:1] == [("start", "t1", "cold")], calls
    assert not any(c[0] == "reset" for c in calls if isinstance(c, tuple)), calls
    assert runs[-1].status == "ok" and runs[-1].user_data["start_at"] == "cold", runs[-1]
    print("ok  start_at: cold starts k6 inside the trigger and never resets")

    # treatment: prepared after the caller's onRunStart, resumed inside the trigger
    a, mod, fake, calls, runs = harness("treatment")
    a.run(script_path=script, download_dir=dl())
    seq = [c if isinstance(c, str) else c[0] for c in calls]
    assert seq.index("caller.onRunStart") < seq.index("start") < seq.index("caller.onTreatmentStart") < seq.index("resume"), seq
    assert ("start", "t1", "paused") in calls and fake.running() == [], calls
    ud = runs[-1].user_data
    assert runs[-1].status == "ok" and ud["resumed_at"] == 2.0 and ud["prepare_s"] is not None, ud
    assert ud["resume_requested_at"] is not None and ud["start_at"] == "treatment", ud
    print("ok  start_at: treatment prepares in onRunStart, after the caller's, and resumes inside the trigger")

    # left_window: resumed in onLeftWindowStart, before the treatment starts
    a, mod, fake, calls, runs = harness("left_window")
    a.run(script_path=script, download_dir=dl())
    seq = [c if isinstance(c, str) else c[0] for c in calls]
    assert seq.index("start") < seq.index("caller.onLeftWindowStart") < seq.index("resume") < seq.index("caller.onTreatmentStart"), seq
    assert runs[-1].status == "ok" and runs[-1].user_data["start_at"] == "left_window" and fake.running() == []
    print("ok  start_at: left_window resumes in onLeftWindowStart, so the warm-up is the same k6 run")

    # a failed resume is a failed run, recorded, with nothing left running
    for start_at, where in (("treatment", "at the treatment"), ("left_window", "at the left window")):
        a, mod, fake, calls, runs = harness(start_at)
        fake.resume_fails = True
        a.run(script_path=script, download_dir=dl())
        r = runs[-1]
        assert r.status == "error" and where in r.user_data["error"] and "502" in r.user_data["error"], r
        assert ("stop", "t1") in calls and fake.running() == [], calls
    print("ok  a failed resume gives status error with the server's reason; the k6 run is stopped")

    # left_window without an observation window: detected in the trigger
    a, mod, fake, calls, runs = harness("left_window", window=False)
    a.run(script_path=script, download_dir=dl())
    assert runs[-1].status == "error" and "observation.window" in runs[-1].user_data["error"], runs[-1]
    assert fake.running() == [], calls
    print("ok  left_window without a window fails the run with an actionable error")

    # a later observer aborts the run after the prepare; the next, idle run kills the paused k6
    a, mod, fake, calls, runs = harness("treatment")
    a.observers.append(Boom())
    try:
        a.run(script_path=script, download_dir=dl()); raise AssertionError("onRunStart is fatal")
    except RuntimeError as exc:
        assert "preflight" in str(exc)
    assert fake.running() == ["t1"], "the aborted run left its paused k6"
    a.observers.pop()
    a.modules["telek6"] = IdleWait()   # an idle cell: another trigger, the module still observes
    a.run(profile_id="idle")
    assert fake.running() == [] and ("reset", ("t1",)) in calls, calls
    assert not any(c[0] == "start" and c[1] != "t1" for c in calls if isinstance(c, tuple)), "an idle run prepares nothing"
    print("ok  a run aborted after the prepare leaves no k6 through the next, idle run")

    # stale parameters cannot leak: the caller aborts before the module's onRunStart
    a, mod, fake, calls, runs = harness("treatment")
    a.observers.insert(0, Boom())
    try:
        a.run(script_path=script, download_dir=dl()); raise AssertionError("onRunStart is fatal")
    except RuntimeError:
        pass
    a.observers.pop(0)
    a.modules["telek6"] = IdleWait()
    a.run(profile_id="idle")
    assert not any(c[0] == "start" for c in calls if isinstance(c, tuple)), calls
    print("ok  parameters of an aborted run are overwritten by the next run's onRunParameters")

    # a direct trigger() outside any SMA run starts cold, also in a prepared mode
    a, mod, fake, calls, runs = harness("left_window")
    meta = mod.trigger(threading.Event(), script_path=script, download_dir=dl())
    assert ("start", "t1", "cold") in calls and meta["start_at"] == "cold" and meta["k6_status"] == "complete", meta
    print("ok  a direct trigger() call with nothing prepared starts k6 cold")

    # the trigger's meta never collides with a run argument
    a, mod, fake, calls, runs = harness("treatment")
    files = {"plan.ndjson": script}
    a.run(script_path=script, files=files, download_dir=dl())
    ud = runs[-1].user_data
    assert ud["files"] == files and ud["downloaded_files"] == ["console.ndjson"], ud
    direct = mod.trigger(threading.Event(), script_path=script, download_dir=dl())
    assert not set(direct) & set(WORKLOAD), set(direct) & set(WORKLOAD)
    print("ok  meta keys and run arguments stay apart in the report")

    # max_duration: SMA gives up on the trigger; its thread must not reset during the next run
    a, mod, fake, calls, runs = harness("treatment", max_duration=1)
    fake.block_wait = True
    a.run(script_path=script)   # temp download dir
    assert runs[-1].status == "timeout", runs[-1]
    fake.block_wait = False
    a.run(script_path=script, download_dir=dl())
    time.sleep(0.6)   # let the abandoned thread stop and download t1
    assert runs[-1].status == "ok" and fake.runs["t2"]["stopped"] is None, fake.runs
    assert fake.off_main_resets == [], fake.off_main_resets
    assert calls.count(("download", "t1")) == 1, "the late thread still downloads its own run"
    print("ok  a trigger thread SMA gave up on stops only its own k6 run and never resets")

    # a pre-0.3 telek6 ignores paused and starts the load at once: refused, and killed
    a, mod, fake, calls, runs = harness("treatment")
    real_start = fake.start_run
    def old_server_start(*args, paused=False, **kw):
        t = real_start(*args, paused=False, **kw)
        del fake.runs[t]["paused"], fake.runs[t]["ready"]   # 0.2.0's status has neither key
        return t
    fake.start_run = old_server_start
    try:
        a.run(script_path=script, download_dir=dl()); raise AssertionError("an old server must fail the prepare")
    except RuntimeError as exc:
        assert "telek6 >= 0.3.0" in str(exc), exc
    assert fake.running() == [] and ("reset", ("t1",)) in calls, calls
    print("ok  a telek6 that ignores paused fails the prepare with the version it needs, and its run is killed")

    # config refusals
    for bad in ({"start_at": "later"}, {"start_at": "left_window", "script_path": script},
                {"start_at": "treatment", "timeout_s": None}):
        try:
            Telek6SmaModule({"url": "http://x", **bad}); raise AssertionError(bad)
        except ValueError:
            pass
    assert Telek6SmaModule({"url": "http://x", "start_at": "treatment", "script_path": script}).config.script_path
    print("ok  config: start_at validated; no config workload with left_window; prepared runs need timeout_s")

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
