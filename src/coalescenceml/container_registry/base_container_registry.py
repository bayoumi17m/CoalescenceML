from typing import ClassVar

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import StackComponent


class BaseContainerRegistry(StackComponent):
    """Base class for all CoalescenceML container registries.
    Attributes:
        uri: The URI of the container registry.
    """

    uri: str

    # Class Configuration
    TYPE: ClassVar[StackComponentFlavor] = StackComponentFlavor.CONTAINER_REGISTRY
    FLAVOR: ClassVar[str] = "default"