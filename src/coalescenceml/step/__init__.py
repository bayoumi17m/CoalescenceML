"""
A step is a single piece or stage of an ML Pipeline. Each step is essentilly
a node of a Directed Acyclic Graph (or DAG). 

Steps can be subclassed from the `BaseStep` class, or used via our `@step` decorator.
"""

from coalescenceml.steps.base_step import BaseStep
from coalescenceml.steps.base_step_config import BaseStepConfig
from coalescenceml.steps.step_context import StepContext
from coalescenceml.steps.step_decorator import step
from coalescenceml.steps.step_environment import STEP_ENVIRONMENT_NAME, StepEnvironment
from coalescenceml.steps.step_output import Output

__all__ = [
    "STEP_ENVIRONMENT_NAME",
    "BaseStep",
    "BaseStepConfig",
    "StepContext",
    "step",
    "Output",
    "StepEnvironment",
]