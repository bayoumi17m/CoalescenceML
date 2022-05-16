from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import StackComponent


class BaseExperimentTracker(StackComponent, ABC):
    """Base class for all CoalescenceML experiment trackers."""

    # Class config
    TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.EXPERIMENT_TRACKER
    FLAVOR: ClassVar[str]


    @abstractmethod
    def log_params(self, params: Dict[str, Any]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def log_artifacts(self, artifacts: Dict[str, Any]) -> None:
        raise NotImplementedError()
