from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import StackComponent
from coalescenceml.pipeline import BasePipeline
from coalescenceml.pipeline.runtime_configuration import RuntimeConfiguration
from coalescenceml.stack import Stack


class BaseOrchestrator(StackComponent, ABC):
    """Base class for all CoalescenceML orchestrators."""

    # Class Configuration
    TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.ORCHESTRATOR

    @abstractmethod
    def run_pipeline(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> Any:
        """Runs a pipeline.
        Args:
            pipeline: The pipeline to run.
            stack: The stack on which the pipeline is run.
            runtime_configuration: Runtime configuration of the pipeline run.
        """