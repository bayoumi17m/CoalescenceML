
from __future__ import annotations

import textwrap
from abc import ABC
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Set
from uuid import UUID, uuid4
from xmlrpc.client import Boolean

from pydantic import BaseModel, Field, root_validator

from coalescenceml.stack import StackComponent
from coalescenceml.enums import StackComponentFlavor

if TYPE_CHECKING:
    from coalescenceml.pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack, StackValidator

"""
Kubeflow Orchestrator model pre-deployment steps
"""


class KubeflowStackValidator(StackValidator):
    """
      Abstract Stack Validator that validates stack compatibility with Kubeflow
    """
    def validate(stack: Stack) -> Boolean:
        """
        Validates the stack to ensure all is compatible with KubeFlow
        """
        pass


class KubeflowStackComponent(StackComponent, ABC):
    """Abstract StackComponent class for all components of a CoalescenceML stack.

    Attributes:
        name: The name of the component.
        uuid: Unique identifier of the component.
    """

    # Class Configuration
    TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.KUBEFLOW
    FLAVOR: ClassVar[str] = "kubeflow"

    def prepare_pipeline_deployment(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> None:
        """Prepares deploying the pipeline.

        Args:
            pipeline: The pipeline to be deployed.
            stack: The stack to be deployed.
            runtime_configuration: The runtime configuration to be used.
        """

        # Containerize all steps using docker utils helper functions
        for step in pipeline.steps:
            # step.containerize()
            # step.componentize() - creates either a string or yaml file that
            # contains the spec for each step for kubeflow
            pass

    @property
    def validator(self) -> StackValidator:
        """Returns the validator for this component."""
        return KubeflowStackValidator()
