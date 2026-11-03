"""
Core data models and protocols for SMA.

This module contains all dataclasses and protocol definitions that can be
safely imported throughout the codebase without circular import issues.
Config classes are kept separate in config.py.
"""
import datetime
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Any, runtime_checkable


# from sma.report import Report # to avoid circular import


# ============================================================================
# Protocols
# ============================================================================
from  threading import Event
class SMAMetadata(Protocol):
    """Protocol for metadata objects that can be serialized to dict."""
    def to_dict(self, kwargs: Optional[dict]) -> dict:
        ...

@runtime_checkable
class Triggerable(Protocol):
    """
      Triggerable Treatment Protocol for SMA modules that to run treatments to observe
    """
    def trigger(self, cancel: threading.Event, **kwargs) -> Optional[Dict[str, Any]]:
        """
            A blocking function that initiates a treatment run.

            must observe the cancle event and block

            returns metadata about the run, defined by the user/module.\

        """
        pass

class SMAObserver(Protocol):
    """Observer protocol for SMA lifecycle events."""

    def onSetup(self) -> None:
        pass

    def onSessionStart(self) -> None:
        pass

    def onRunParameters(self, **kwargs) -> None:
        """Called right before onRunStart with the keyword arguments of this run,
        exactly as the trigger will receive them. Not fatal. An observer that
        needs per-run input before the treatment (e.g. to prepare a load
        generator in onRunStart) keeps a copy; every run overwrites it."""
        pass

    def onRunStart(self) -> None:
        pass

    def onLeftWindowStart(self) -> None:
        pass

    def onLeftWindowEnd(self) -> None:
        pass

    def onTreatmentStart(self) -> None:
        pass

    def onTreatmentEnd(self) -> None:
        pass

    def onRightWindowStart(self) -> None:
        pass

    def onRightWindowEnd(self) -> None:
        pass

    # should mention the passing of report but import will be circular, dealing with that later
    def onReport(self, report=None) -> None: 
        pass

    def onRunEnd(self, run=None) -> None:
        pass

    def onSessionEnd(self) -> None:
        pass

    def onTeardown(self) -> None:
        pass

    def onRunTimeout(self) -> None:
        pass



# ============================================================================
# Report Models
# ============================================================================

@dataclass
class SMASession:
    """
    Metadata about the measurement session (initialized once when an SMA agent is created).
    """
    name: str
    extras: Optional[dict] = None

    def to_dict(self, kwargs: Optional[dict]) -> dict:
        meta = {
            "session": self.name,
        }

        # Merge extras at top level for template substitution
        if self.extras:
            meta.update(self.extras)

        if kwargs:
            meta.update(kwargs)
        return meta

@dataclass
class SMARun:
    """
    Metadata about a specific measurement run.
    """
    startTime: datetime.datetime
    endTime: datetime.datetime
    treatment_start: datetime.datetime
    treatment_end: datetime.datetime
    runHash: str
    user_data: Optional[dict] = None
    status: Optional[str] = None

    def duration(self) -> datetime.timedelta:
        return self.endTime - self.startTime

    def treatment_duration(self) -> datetime.timedelta:
        return self.treatment_end - self.treatment_start

    def to_dict(self, kwargs: Optional[dict]) -> dict:
        meta = {
            "startTime": self.startTime.strftime("%Y_%m_%d_%H_%M_%S"),
            "endTime": self.endTime.strftime("%Y_%m_%d_%H_%M_%S") if self.endTime is not None else "",
            "treatment_start": self.treatment_start.strftime("%Y_%m_%d_%H_%M_%S") if self.treatment_start is not None else "",
            "treatment_end": self.treatment_end.strftime("%Y_%m_%d_%H_%M_%S") if self.treatment_end is not None else "",
            "runHash": self.runHash,
            "duration": self.duration().total_seconds() if self.duration() is not None else "",  # type: ignore
            "treatment_duration": self.treatment_duration().total_seconds() if self.treatment_duration() is not None else "",  # type: ignore
            "status": self.status if self.status is not None else "unknown",
            "user_data": self.user_data if self.user_data is not None else {},
        }

        if kwargs:
            meta.update(kwargs)
        return meta

    @staticmethod
    def fields() -> List[str]:
        return [
            "startTime",
            "endTime",
            "treatment_start",
            "treatment_end",
            "runHash",
            "duration",
            "treatment_duration",
            "user_data",
            "status"
        ]


@dataclass
class ReportMetadata:
    """Combined metadata for a report (session + run)."""
    session: SMASession
    run: SMARun

    def to_dict(self, kwargs: Optional[dict] = None) -> dict:
        meta = {}
        meta.update(self.session.to_dict({}))
        meta.update(self.run.to_dict({}))
        if kwargs:
            meta.update(kwargs)
        return meta


# ============================================================================
# Observation Models
# ============================================================================

@dataclass
class ObservationTarget:
    """Target specification for observations (based on Kubernetes label selectors)."""
    match_labels: Optional[dict] = None
    match_expressions: Optional[list] = None


@dataclass
class ObservationWindow:
    """Time window configuration for observations."""
    left: int
    right: int

@dataclass
class ObservationConfig:
    """Configuration for observation behavior."""
    mode: str
    window: Optional[ObservationWindow]
    max_duration : Optional[int] = None
    module_trigger: Optional[str] = None
    targets: Optional[List[ObservationTarget]] = None



# ============================================================================
# Config Models (non-Config dataclasses)
# ============================================================================

@dataclass
class ReportConfig:
    """Configuration for report generation and storage."""
    format: str = "csv"
    location: str = "reports/${startTime}_${runHash}/"
    filename: str = "${name}.csv"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReportConfig":
        """Create ReportConfig from a dictionary."""
        return cls(
            format=data.get("format", "csv"),
            location=data.get("location", "reports/${startTime}_${runHash}/"),
            filename=data.get("filename", "${name}.csv")
        )


@dataclass
class MeasurementConfig:
    """Configuration for a single measurement."""
    name: str
    type: str
    query: str
    step: int = 60
    layer: Optional[str] = None
    unit: Optional[str] = None
    target_names: Optional[List[str]] = None

