from collections import defaultdict
from typing import DefaultDict, Dict

from pydantic import BaseModel, validator

from coalescenceml.enums import StackComponentFlavor


class StackStoreModel(BaseModel):
    """Pydantic object used for serializing a CoalescenceML Stack Store.

    Attributes:
        version: coalescenceml version number
        stacks: Maps stack names to a configuration object containing the
            names and flavors of all stack components.
        stack_components: Contains names and flavors of all registered stack
            components.
    """

    stacks: Dict[str, Dict[StackComponentFlavor, str]]
    stack_components: DefaultDict[StackComponentFlavor, Dict[str, str]]

    @validator("stack_components")
    def _construct_defaultdict(
        cls, stack_components: Dict[StackComponentFlavor, Dict[str, str]]
    ) -> DefaultDict[StackComponentFlavor, Dict[str, str]]:
        """Ensures that `stack_components` is a defaultdict so stack
        components of a new component type can be added without issues."""
        return defaultdict(dict, stack_components)

    @classmethod
    def empty_store(cls) -> "StackStoreModel":
        """Initialize a new empty stack store with current zen version."""
        return cls(stacks={}, stack_components={})

    class Config:
        """Pydantic configuration class."""

        # Validate attributes when assigning them. We need to set this in order
        # to have a mix of mutable and immutable attributes
        validate_assignment = True
        # Ignore extra attributes from configs of previous CoalescenceML versions
        extra = "ignore"
