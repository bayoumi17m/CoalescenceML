from collections import defaultdict
from typing import ClassVar, DefaultDict, Dict, Type, TypeVar

from coalescenceml.artifact_store import LocalArtifactStore
from coalescenceml.container_registry import BaseContainerRegistry
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.logger import get_logger
from coalescenceml.metadata_store import MySQLMetadataStore, SQLiteMetadataStore
from coalescenceml.orchestrator import LocalOrchestrator
from coalescenceml.stack import StackComponent

logger = get_logger(__name__)


class StackComponentClassRegistry:
    """Registry for stack component classes.
    All stack component classes must be registered here, so they can be
    instantiated from the component type and flavor specified inside the
    CoalescenceML directory configuration.
    """

    component_classes: ClassVar[
        DefaultDict[StackComponentFlavor, Dict[str, Type[StackComponent]]]
    ] = defaultdict(dict)

    @classmethod
    def register_class(
        cls,
        component_class: Type[StackComponent],
    ) -> None:
        """Registers a stack component class."""
        component_flavor = component_class.FLAVOR
        flavors = cls.component_classes[component_class.TYPE]
        if component_flavor in flavors:
            logger.warning(
                "Overwriting previously registered stack component class `%s` "
                "for type '%s' and flavor '%s'.",
                flavors[component_flavor].__class__.__name__,
                component_class.TYPE.value,
                component_class.FLAVOR,
            )

        flavors[component_flavor] = component_class
        logger.debug(
            "Registered stack component class for type '%s' and flavor '%s'.",
            component_class.TYPE.value,
            component_flavor,
        )

    @classmethod
    def get_class(
        cls,
        component_type: StackComponentFlavor,
        component_flavor: str,
    ) -> Type[StackComponent]:
        """Returns the stack component class for the given type and flavor.
        
        Args:
            component_type: The type of the component class to return.
            component_flavor: The flavor of the component class to return.
        Raises:
            KeyError: If no component class is registered for the given type
                and flavor.
        """

        available_flavors = cls.component_classes[component_type]
        try:
            return available_flavors[component_flavor]
        except KeyError:
            # The stack component might be part of an integration
            # -> Activate the integrations and try again
            from coalescenceml.integrations.registry import integration_registry

            integration_registry.activate_integrations()

            try:
                return available_flavors[component_flavor]
            except KeyError:
                raise KeyError(
                    f"No stack component class found for type {component_type} "
                    f"and flavor {component_flavor}. Registered flavors for "
                    f"this type: {set(available_flavors)}. If your stack "
                    f"component class is part of a CoalescenceML integration, make "
                    f"sure the corresponding integration is installed by "
                    f"running `coml integration install INTEGRATION_NAME`."
                ) from None


C = TypeVar("C", bound=StackComponent)


def register_stack_component_class(cls: Type[C]) -> Type[C]:
    """Registers the stack component class and returns it unmodified."""
    StackComponentClassRegistry.register_class(component_class=cls)
    return cls


StackComponentClassRegistry.register_class(LocalOrchestrator)
StackComponentClassRegistry.register_class(SQLiteMetadataStore)
StackComponentClassRegistry.register_class(MySQLMetadataStore)
StackComponentClassRegistry.register_class(LocalArtifactStore)
StackComponentClassRegistry.register_class(BaseContainerRegistry)