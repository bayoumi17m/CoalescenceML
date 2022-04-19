from coalescenceml.container_registry import BaseContainerRegistry
from coalescenceml.enums import StackComponentFlavor


def test_base_container_registry_attributes():
    """Tests that the basic attributes of the base container registry are set
    correctly."""
    container_registry = BaseContainerRegistry(name="", uri="")
    assert container_registry.TYPE == StackComponentFlavor.CONTAINER_REGISTRY
    assert container_registry.FLAVOR == "default"