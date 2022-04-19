from abc import ABC, abstractmethod
from typing import ClassVar, List

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import StackComponent


class BaseStepOperator(StackComponent, ABC):
    """Base class for all CoalescenceML step operators."""

    # Class Configuration
    TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.STEP_OPERATOR

    @abstractmethod
    def launch(
        self,
        pipeline_name: str,
        run_name: str,
        requirements: List[str],
        entrypoint_command: List[str],
    ) -> None:
        """Abstract method to execute a step.

        Concrete step operator subclasses must implement the following
        functionality in this method:
        - Prepare the execution environment and install all the necessary
          `requirements`
        - Launch a **synchronous** job that executes the `entrypoint_command`

        Args:
            pipeline_name: Name of the pipeline which the step to be executed
                is part of.
            run_name: Name of the pipeline run which the step to be executed
                is part of.
            entrypoint_command: Command that executes the step.
            requirements: List of pip requirements that must be installed
                inside the step operator environment.
        """
