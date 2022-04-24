from typing import List

from pydantic import BaseModel

from coalescenceml.stack import Stack
from coalescenceml.stack_store.model import StackComponentWrapper


class StackWrapper(BaseModel):
    """Network Serializable Wrapper describing a Stack."""

    name: str
    components: List[StackComponentWrapper]

    @classmethod
    def from_stack(cls, stack: Stack) -> "StackWrapper":
        return cls(
            name=stack.name,
            components=[
                StackComponentWrapper.from_component(component)
                for t, component in stack.components.items()
            ],
        )
