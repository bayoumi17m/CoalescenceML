from __future__ import annotations

import textwrap
from abc import ABC
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Set
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, root_validator

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.integrations.utils import get_requirements_for_module
from coalescenceml.stack.exceptions import StackComponentInterfaceError


if TYPE_CHECKING:
    from coalescenceml.pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack, StackValidator


class StackComponent(BaseModel, ABC):
    """Abstract StackComponent class for all components of a CoalescenceML stack.

    Attributes:
        name: The name of the component.
        uuid: Unique identifier of the component.
    """

    name: str
    uuid: UUID = Field(default_factory=uuid4)

    # Class Configuration
    TYPE: ClassVar[StackComponentFlavor]
    FLAVOR: ClassVar[str]

    @property
    def log_file(self) -> Optional[str]:
        """Optional path to a log file for the stack component."""
        # TODO: Add support for multiple log files for a stack
        #  component. E.g. let each component return a generator that yields
        #  logs instead of specifying a single file path.
        return None

    @property
    def runtime_options(self) -> Dict[str, Any]:
        """Runtime options that are available to configure this component.

        The items of the dictionary should map option names (which can be used
        to configure the option in the `RuntimeConfiguration`) to default
        values for the option (or `None` if there is no default value).
        """
        return {}

    @property
    def requirements(self) -> Set[str]:
        """Set of PyPI requirements for the component."""
        return set(get_requirements_for_module(self.__module__))

    def prepare_pipeline_deployment(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> None:
        """Prepares deploying the pipeline.

        This method gets called immediately before a pipeline is deployed.
        Subclasses should override it if they require runtime configuration
        options or if they need to run code before the pipeline deployment.

        Args:
            pipeline: The pipeline that will be deployed.
            stack: The stack on which the pipeline will be deployed.
            runtime_configuration: Contains all the runtime configuration
                options specified for the pipeline run.
        """

    def prepare_pipeline_run(self) -> None:
        """Prepares running the pipeline."""

    def cleanup_pipeline_run(self) -> None:
        """Cleans up resources after the pipeline run is finished."""

    @property
    def validator(self) -> Optional["StackValidator"]:
        """The optional validator of the stack component.

        This validator will be called each time a stack with the stack
        component is initialized. Subclasses should override this property
        and return a `StackValidator` that makes sure they're not included in
        any stack that they're not compatible with.
        """
        return None

    @property
    def is_provisioned(self) -> bool:
        """If the component provisioned resources to run locally."""
        return True

    @property
    def is_running(self) -> bool:
        """If the component is running locally."""
        return True

    @property
    def is_suspended(self) -> bool:
        """If the component is suspended."""
        return not self.is_running

    def provision(self) -> None:
        """Provisions resources to run the component locally."""
        raise NotImplementedError(
            f"Provisioning local resources not implemented for {self}."
        )

    def deprovision(self) -> None:
        """Deprovisions all local resources of the component."""
        raise NotImplementedError(
            f"Deprovisioning local resource not implemented for {self}."
        )

    def resume(self) -> None:
        """Resumes the provisioned local resources of the component."""
        raise NotImplementedError(
            f"Resuming provisioned resources not implemented for {self}."
        )

    def suspend(self) -> None:
        """Suspends the provisioned local resources of the component."""
        raise NotImplementedError(
            f"Suspending provisioned resources not implemented for {self}."
        )

    def __repr__(self) -> str:
        """String representation of the stack component."""
        attribute_representation = ", ".join(
            f"{key}={value}" for key, value in self.dict().items()
        )
        return (
            f"{self.__class__.__qualname__}(type={self.TYPE}, "
            f"flavor={self.FLAVOR}, {attribute_representation})"
        )

    def __str__(self) -> str:
        """String representation of the stack component."""
        return self.__repr__()

    @root_validator
    def _ensure_stack_component_complete(cls, values: Dict[str, Any]) -> Any:
        try:
            stack_component_flavor = getattr(cls, "TYPE")
            assert stack_component_flavor in StackComponentFlavor
        except (AttributeError, AssertionError):
            raise StackComponentInterfaceError(
                textwrap.dedent(
                    """
                    When you are working with any classes which subclass from
                    `coalescenceml.stack.StackComponent` please make sure that your
                    class has a ClassVar named `TYPE` and its value is set to a
                    `StackComponentFlavor` from `from coalescenceml.enums import StackComponentFlavor`.

                    In most of the cases, this is already done for you within the
                    implementation of the base concept.

                    Example:

                    class BaseArtifactStore(StackComponent):
                        # Instance Variables
                        path: str

                        # Class Variables
                        TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.ARTIFACT_STORE
                    """
                )
            )

        try:
            getattr(cls, "FLAVOR")
        except AttributeError:
            raise StackComponentInterfaceError(
                textwrap.dedent(
                    """
                    When you are working with any classes which subclass from
                    `coalescenceml.stack.StackComponent` please make sure that your
                    class has a defined ClassVar `FLAVOR`.

                    Example:

                    class LocalArtifactStore(BaseArtifactStore):

                        ...

                        # Define flavor as a ClassVar
                        FLAVOR: ClassVar[str] = "local"

                        ...
                    """
                )
            )

        return values

    class Config:
        """Pydantic configuration class."""

        # public attributes are immutable
        allow_mutation = False
        # all attributes with leading underscore are private and therefore
        # are mutable and not included in serialization
        underscore_attrs_are_private = True
