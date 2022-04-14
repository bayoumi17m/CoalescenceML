"""
A step is a single piece or stage of an ML Pipeline. Each step is essentilly
a node of a Directed Acyclic Graph (or DAG). 

Steps can be subclassed from the `BaseStep` class, or used via our `@step` decorator.
"""

from coalescenceml.step.base_step import BaseStep
from coalescenceml.step.base_step_config import BaseStepConfig
from coalescenceml.step.step_context import StepContext
from coalescenceml.step.step_decorator import step
from coalescenceml.step.step_environment import STEP_ENVIRONMENT_NAME, StepEnvironment
from coalescenceml.step.output import Output

__all__ = [
    "STEP_ENVIRONMENT_NAME",
    "BaseStep",
    "BaseStepConfig",
    "StepContext",
    "step",
    "Output",
    "StepEnvironment",
]