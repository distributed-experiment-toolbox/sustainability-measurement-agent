"""Observer notification: onRunStart failures abort the run, other hooks only log;
onRunParameters reaches observers right before onRunStart without affecting those
that do not implement it.

python -m tests.test_notify_observers
"""
import logging
import threading
from types import SimpleNamespace

from sma.model import SMAObserver, SMASession
from sma.sma import SustainabilityMeasurementAgent


class Boom:
    def onRunStart(self): raise RuntimeError("skaffold run failed")
    def onReport(self, report=None): raise RuntimeError("cosmetic")


class Errors(logging.Handler):
    def __init__(self):
        super().__init__(logging.ERROR)
        self.seen = []

    def emit(self, record):
        self.seen.append(record.getMessage())


def bare_agent(observers):
    sma = SustainabilityMeasurementAgent.__new__(SustainabilityMeasurementAgent)
    sma.observers = observers
    sma.logger = logging.getLogger("t")
    return sma


def test_on_run_start_propagates():
    sma = bare_agent([Boom()])
    try:
        sma.notify_observers("onRunStart"); assert False, "onRunStart error must propagate"
    except RuntimeError as e:
        assert "skaffold" in str(e)
    sma.notify_observers("onReport")   # non-fatal events still only log
    assert SustainabilityMeasurementAgent.FATAL_EVENTS == ("onRunStart",)


def test_on_run_parameters_is_opt_in():
    """Observers without the hook, and SMAObserver subclasses that inherit the
    no-op, see no error; a run argument named `event` reaches the hook."""
    class Plain:              # no onRunParameters at all
        pass

    class Sub(SMAObserver):   # inherits the protocol's no-op default
        pass

    class Wants:
        def onRunParameters(self, **kwargs): self.got = kwargs

    wants, errors = Wants(), Errors()
    sma = bare_agent([Plain(), Sub(), wants])
    sma.logger.addHandler(errors)
    try:
        sma.notify_observers("onRunParameters", script_path="s.js", event="x")
    finally:
        sma.logger.removeHandler(errors)
    assert errors.seen == [], errors.seen
    assert wants.got == {"script_path": "s.js", "event": "x"}, wants.got


def test_run_fires_on_run_parameters_before_on_run_start():
    seen = []

    class Recorder:
        def onRunParameters(self, **kwargs): seen.append(("onRunParameters", kwargs))
        def onRunStart(self): seen.append(("onRunStart", None))

    class Trigger:
        def trigger(self, cancel: threading.Event, **kwargs):
            seen.append(("trigger", kwargs))
            return {}

    sma = bare_agent([Recorder()])
    sma.config = SimpleNamespace(observation=SimpleNamespace(mode="trigger", max_duration=None, window=None))
    sma.session = SMASession(name="t")
    sma.observe_once = lambda run_data, overwrite=False: None
    sma.run(Trigger(), profile_id="p1", seed=7)
    assert [e for e, _ in seen] == ["onRunParameters", "onRunStart", "trigger"], seen
    assert seen[0][1] == seen[2][1] == {"profile_id": "p1", "seed": 7}, seen


if __name__ == "__main__":
    test_on_run_start_propagates()
    test_on_run_parameters_is_opt_in()
    test_run_fires_on_run_parameters_before_on_run_start()
    print("ok")
