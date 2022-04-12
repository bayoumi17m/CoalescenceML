"""
## Stack
The stack is essentially all the configuration for the infrastructure of your 
MLOps platform.
A stack is made up of multiple components. Some examples are:
- An Artifact Store
- A Metadata Store
- An Orchestrator
- A Container Registry (Optional)
"""

from coalescenceml.stack.stack import Stack
from coalescenceml.stack.stack_component import StackComponent
from coalescenceml.stack.stack_validator import StackValidator

__all__ = [
    "Stack",
    "StackComponent",
    "StackValidator",
]