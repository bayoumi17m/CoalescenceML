"""
## Container Registry

A container registry is a store for (Docker) containers. A CoalescenceML workflow
involving a container registry would automatically containerize your code to
be transported across stacks running remotely
"""
from coalescenceml.container_registry.base_container_registry import (
    BaseContainerRegistry,
)


__all__ = ["BaseContainerRegistry"]
