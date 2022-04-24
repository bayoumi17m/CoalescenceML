from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List

from ml_metadata import proto

from coalescenceml.enums import ExecutionStatus
from coalescenceml.logger import get_logger


if TYPE_CHECKING:
    from coalescenceml.metadata_store import BaseMetadataStore
    from coalescenceml.post_execution.step import StepView

logger = get_logger(__name__)


class PipelineRunView:
    """Post-execution pipeline run class which can be used to query
    steps and artifact information associated with a pipeline execution.
    """

    def __init__(
        self,
        id_: int,
        name: str,
        executions: List[proto.Execution],
        metadata_store: BaseMetadataStore,
    ):
        """Initializes a post-execution pipeline run object.
        In most cases `PipelineRunView` objects should not be created manually
        but retrieved from a `PipelineView` object instead.
        Args:
            id_: The context id of this pipeline run.
            name: The name of this pipeline run.
            executions: All executions associated with this pipeline run.
            metadata_store: The metadata store which should be used to fetch
                additional information related to this pipeline run.
        """
        self._id = id_
        self._name = name
        self._metadata_store = metadata_store

        self._executions = executions
        self._steps: Dict[str, StepView] = OrderedDict()

    @property
    def name(self) -> str:
        """Returns the name of the pipeline run."""
        return self._name

    @property
    def status(self) -> ExecutionStatus:
        """Returns the current status of the pipeline run."""
        step_statuses = (step.status for step in self.steps)

        if any(status == ExecutionStatus.FAILED for status in step_statuses):
            return ExecutionStatus.FAILED
        elif all(
            status == ExecutionStatus.COMPLETED
            or status == ExecutionStatus.CACHED
            for status in step_statuses
        ):
            return ExecutionStatus.COMPLETED
        else:
            return ExecutionStatus.RUNNING

    @property
    def steps(self) -> List[StepView]:
        """Returns all steps that were executed as part of this pipeline run."""
        self._ensure_steps_fetched()
        return list(self._steps.values())

    def get_step_names(self) -> List[str]:
        """Returns a list of all step names."""
        self._ensure_steps_fetched()
        return list(self._steps.keys())

    def get_step(self, name: str) -> StepView:
        """Returns a step for the given name.
        Args:
            name: The name of the step to return.
        Raises:
            KeyError: If there is no step with the given name.
        """
        self._ensure_steps_fetched()
        try:
            return self._steps[name]
        except KeyError:
            raise KeyError(
                f"No step found for name `{name}`. This pipeline "
                f"run only has steps with the following "
                f"names: `{self.get_step_names()}`"
            )

    def _ensure_steps_fetched(self) -> None:
        """Fetches all steps for this pipeline run from the metadata store."""
        if self._steps:
            # we already fetched the steps, no need to do anything
            return

        self._steps = self._metadata_store.get_pipeline_run_steps(self)

    def __repr__(self) -> str:
        """Returns a string representation of this pipeline run."""
        return (
            f"{self.__class__.__qualname__}(id={self._id}, "
            f"name='{self._name}')"
        )

    def __eq__(self, other: Any) -> bool:
        """Returns whether the other object is referring to the same
        pipeline run."""
        if isinstance(other, PipelineRunView):
            return (
                self._id == other._id
                and self._metadata_store.uuid == other._metadata_store.uuid
            )
        return False
