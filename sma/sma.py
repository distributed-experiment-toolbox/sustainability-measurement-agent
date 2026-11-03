
import datetime
import hashlib
import importlib
import queue
import random
import threading
from contextlib import contextmanager
from logging import Logger, getLogger
from time import sleep
from typing import Generator, Optional, Dict, Any, Tuple, List

from sma import __version__
from sma.config import Config
from sma.model import (
    SMAObserver, Triggerable,
    SMARun, SMASession, ReportMetadata
)
from sma.report import Report
from sma.service import ServiceException

# hook names SMA fired before 0.2 -> the SMAObserver protocol's names
DEPRECATED_EVENTS = {"onLeftWindowStart": "onLeftStart", "onLeftWindowEnd": "onLeftEnd",
                     "onRightWindowStart": "onRightStart", "onRightWindowEnd": "onRightEnd"}


def make_run_hash(start_time: datetime.datetime) -> str:
    hash_input = f"{start_time.isoformat()}_{random.random()}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:8]


class TimeTrigger(Triggerable):
    def trigger(self, cancel: threading.Event, **kwargs) -> dict:
        import time
        if kwargs and "duration" in kwargs:
            seconds = kwargs["duration"]
            while not cancel.is_set():
                time.sleep(1)
                seconds -= 1
                if seconds <= 0:
                    break
        else:
            raise ValueError("duration must be provided as keyword argument")
        return {}

class SustainabilityMeasurementAgent(object):

    FATAL_EVENTS = ("onRunStart",)   # a run whose setup failed must not be measured

    def __init__(self, config: Config,
                 observers: Optional[List[SMAObserver]] = None, meta: SMASession = None, init_modules: Dict[str, Dict[str, Any]] = {}) -> None:
        """
            Initialize the SMA agent.

            config: SMA configuration object.
            observers: List of SMAObserver objects to notify of agent events, can also be registered later via register_sma_observer.
            meta: Optional SMASession object to use for metadata
            init_modules: Optional dictionary to define modules and configs to be loaded by the agent at startup.
        """
        self.config = config
        self.logger: Logger = getLogger("sma.agent")
        self.logger.debug(f"Running SMA {__version__} with config: {config}")
        self.observers: list[SMAObserver] = [] if observers is None else observers

        self.session: Optional[SMASession] = None
        self.meta_session: Optional[SMASession] = meta

        self.modules = {} | init_modules
        self.load_modules()

        # if meta is not None:
        # now always call setup with or without given session:
        # self.setup(meta) ---> now happening in start_session context manager

    
    @contextmanager
    def start_session(self) -> Generator[None, None, None]:
        self.logger.debug("Starting session...")
        try:
            if self.meta_session:
                self.setup(self.meta_session)
            else:
                self.setup(SMASession(name=f"SMA Session {datetime.datetime.now().isoformat()}"))
            self.notify_observers("onSessionStart")
            yield 
        finally:
            self.notify_observers("onSessionEnd")
            self.teardown()

    def load_modules(self) -> None:
        """Load SMA modules as per configuration and reguister them as observers."""
        #TODO: should be more typed so we can enable easier external loading.
        #TODO: Should we enable free loading also of user code?
        for module_id, module_config in self.config.modules.items():
            self.logger.info(f"Loading module: {module_id}")
            module_name = module_config.get("module")
            if not module_name:
                self.logger.error(f"Module configuration for {module_id} missing 'module' key.")
                continue

            module_cfg = module_config.get("config", {})
            path = f"modules.{module_name}.main"
            klass_name = f"{module_name.capitalize()}SmaModule"  # Placeholder; in real code, determine class name dynamically
            self.logger.debug(f"Importing module class {klass_name} from {path}...")
            
            module = importlib.import_module(path)
            klass = getattr(module, klass_name)
            module_instance = klass(module_cfg)
            self.register_sma_observer(module_instance)
            self.modules[module_id] = module_instance

        self.logger.info(f"Loaded modules: {list(self.modules)}")


    def setup(self, session:SMASession):
        self.notify_observers("onSetup")
        self.logger.debug("Setting up logger with session {session}")
        self.session = session

    def notify_observers(self, event: str, /, **kwargs) -> None:
        """Call `event` on every observer, in registration order: observers passed
        to the constructor first, then config-loaded modules, then observers added
        with register_sma_observer. `event` is positional-only, so a run argument
        named "event" reaches onRunParameters instead of colliding with it."""
        self.logger.info(f"Notifying observers of event: {event}")
        if not hasattr(SMAObserver, event):
            self.logger.warning(f"Observer protocol does not define event {event}")

        for observer in self.observers:
            method = getattr(observer, event, None)
            if method is None and event in DEPRECATED_EVENTS:
                method = getattr(observer, DEPRECATED_EVENTS[event], None)
                if method is not None:
                    self.logger.warning(f"{type(observer).__name__} uses deprecated hook "
                                        f"{DEPRECATED_EVENTS[event]}; rename it to {event}")
            if callable(method):
                try:
                    method(**kwargs)
                except Exception as e:
                    if event in self.FATAL_EVENTS:
                        raise
                    self.logger.error(f"Error calling event {event} on observer {observer}: {e}")
            else:
                self.logger.debug(f"tried calling inexistent event {event} on observer {observer}")

    def register_sma_observer(self, observer: SMAObserver) -> None:
        self.observers.append(observer)

    def unregister_sma_observer(self, observer: SMAObserver) -> None:
        self.observers.remove(observer)

    def connect(self) -> None:
        for k, client in self.config.services.items():
            self.logger.debug(f"Pinging service {k}...")
            if client.ping():
               self.logger.info(f"Service {k} is reachable.")
            else:
               self.logger.error(f"Service {k} is not reachable.")
               raise ValueError(f"Service {k} is not reachable.")
            

    def probe(self) -> dict[str, bool]:
        """Probe all measurements to check if they are available.
        
        Returns:
            A dictionary mapping measurement names to their availability status.
        """
        results = {}
        queries = self.config.measurement_queries()
        
        for name, measurement in queries.items():   
            self.logger.debug(f"Probing measurement {name}: {measurement}")
            try:
                available = measurement.probe()
                results[name] = available
            except ServiceException as e:
                self.logger.error(f"Error probing measurement {name}: {e}")
                results[name] = False   
                 
        return results


    def _run(self, max_duration: Optional[int], trigger: Triggerable, grace_period=0.2,  **kwargs) -> Optional[Tuple[str,Dict[str, Any]]]:
        cancel = threading.Event()
        q = queue.Queue()

        def _worker():
            try:
                meta = trigger.trigger(cancel, **kwargs)
                q.put((200,meta))
            except Exception as e:
                q.put((500,{"error": str(e)}))

        t = threading.Thread(target=_worker, daemon=True)
        t.start()

        try:
            status, meta = q.get(timeout=max_duration)
            if status == 200:
                return "ok", meta
            else:
                raise Exception(meta["error"])
        except queue.Empty:
            cancel.set()
            self.notify_observers("onRunTimeout")
            t.join(timeout=grace_period) #XXX Not sure this works...
            return "timeout", {}
        finally:
            cancel.set()
            t.join(timeout=grace_period)
        






    def run(self, trigger: Optional[Triggerable] = None, **kwargs ) -> None:
        """Execute a measurement run.
        
        Args:
            trigger: Optional function for trigger/continuous modes that returns file-level metadata
        """

        assert self.session is not None, "Session must be set before running, use with start_session context manager."

        mode = self.config.observation.mode
        self.logger.info(f"Observation mode: {mode}")

        max_duration = self.config.observation.max_duration
        if max_duration:
            self.logger.info(f"Max duration: {max_duration}s")

        window = self.config.observation.window
        if window is not None:
            self.logger.info(f"Observation window: left={window.left}s, right={window.right}s") # type: ignore

        if self.session is None:
            raise ValueError("Session must be set before running in trigger mode.")
        
        # checks:
        if mode == "timer":
            assert window is not None, "Observation window must be defined for timer mode"
            assert max_duration is not None, "Max duration must be defined for timer mode"
        elif mode == "trigger":
            if trigger is None:
                raise ValueError("Trigger function must be provided for trigger mode")
        elif mode == "module":
            if trigger is not None:
                self.logger.warning("Trigger function will be ignored in module mode")
            md = self.modules[self.config.observation.module_trigger]
            assert hasattr(md, 'trigger') and callable(getattr(md, 'trigger')), \
                f"Module {md} must have a callable 'trigger' function"
            # assert isinstance(md, Triggerable), f"Module does not implement Triggerable interface"
        else:
            raise ValueError(f"Unknown observation mode: {mode}")

        if mode == "timer":
            trigger = TimeTrigger()
            kwargs["duration"] = int(max_duration)
        elif mode == "module":
            module_id = self.config.observation.module_trigger
            assert module_id is not None
            self.logger.info(f"Waiting for module trigger from module: {module_id}")
            module = self.modules[module_id]

            # we are now specifying the OBJECT, not the function
            # itself, trigger function will be called automatically
            trigger = module  # .trigger

        assert trigger is not None, "Trigger must be defined."

        start_time = datetime.datetime.now()
        run_hash = make_run_hash(start_time)
        self.notify_observers("onRunParameters", **kwargs)
        self.notify_observers("onRunStart")


        if window is not None:
            self.notify_observers("onLeftWindowStart")
            self.logger.info(f"Waiting for left window: {window.left}s")
            sleep(window.left)
            self.notify_observers("onLeftWindowEnd")

        self.logger.info(f"Starting Treatment Phase")
        self.notify_observers("onTreatmentStart")
        treatment_start = datetime.datetime.now()


        try:
            if max_duration:
                status, trigger_meta = self._run(max_duration, trigger, grace_period=0.2, **kwargs)
            else:
                trigger_meta = trigger.trigger(cancel=threading.Event(), **kwargs)
                status = "ok"
        except Exception as e:
            status = "error"
            trigger_meta = {
                "error": str(e)
            }
        self.logger.info(f"Trigger execution finished with status: {status}")
        self.logger.debug(f"Trigger metadata: {trigger_meta}")

        if trigger_meta is None:
            trigger_meta = {}

        treatment_end = datetime.datetime.now()

        self.logger.info(f"Treatment duration: {treatment_end - treatment_start}")
        self.notify_observers("onTreatmentEnd")

        if window is not None:
            self.notify_observers("onRightWindowStart")
            self.logger.info(f"Waiting for right window: {window.right}s")
            sleep(window.right)
            self.notify_observers("onRightWindowEnd")
        end_time = datetime.datetime.now()

        if kwargs is not None:
            trigger_meta = trigger_meta | kwargs

        self.logger.info(f"Total observation duration: {end_time - start_time}")
        run_data = SMARun(
            startTime=start_time,
            endTime=end_time,
            treatment_start=treatment_start,
            treatment_end=treatment_end,
            runHash=run_hash,
            status=status,
            user_data=trigger_meta,
        )
        
        self.observe_once(ReportMetadata(
            session=self.session,
            run=run_data,
        ))

        self.notify_observers("onRunEnd", run=run_data)

    def observe_once(self, run_data: ReportMetadata, overwrite: bool = False) -> None:
        """Observe (i.e., retrieve from target) measurements for a completed run and persist to disk.
        
        Args:
            run_data: ReportMetadata containing timing and run identification data
            overwrite: If True, existing reports will be overwritten, otherwise they will be skipped.
        """
        rep = Report(metadata=run_data, config=self.config, data={})

        queries = self.config.measurement_queries()
        
        for name, measurement in queries.items():   
            try:
                self.logger.info(f"Querying measurement: {name}")
                #XXX: not a good pattern... should not use knowledge about internals of measurment..., labeling should happen during observation
                df = measurement.observe(start=run_data.run.startTime, end=run_data.run.endTime)
                df = measurement.label(treatment_start=run_data.run.treatment_start.timestamp(), treatment_end=run_data.run.treatment_end.timestamp(),
                                       label_column="treatment", label="Treatment")
                rep.set_data(name, df)
            except ServiceException as e:
                self.logger.error(f"Error Querying measurement {name}: {e}")
                if rep.run_data.status == "ok":   # a failed trigger outranks a failed query
                    rep.run_data.status = "partial"
                #TODO: we need to document the meaning of this.

        #TODO: don't like this path through to the ReportIO but ...
        rep.persist(overwrite=overwrite)
        self.notify_observers("onReport", report=rep)

    def deploy(self) -> None:
        raise NotImplementedError("Deployment is not yet implemented.") # todo: as module?
    
    def undeploy(self) -> None:
        raise NotImplementedError("Undeployment is not yet implemented.")
    
    def verify_deployment(self) -> bool:
        raise NotImplementedError("Deployment verification is not yet implemented.")

    # now handled via context manager 
    def teardown(self) -> None:
        self.notify_observers("onTeardown")

