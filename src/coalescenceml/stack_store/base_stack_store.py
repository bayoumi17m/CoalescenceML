import base64
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from coalescenceml.enums import StackComponentFlavor, StoreType
from coalescenceml.stack.exceptions import StackComponentExistsError, StackExistsError
from coalescenceml.logger import get_logger
from coalescenceml.stack import Stack
from coalescenceml.stack_stores.models import StackComponentWrapper, StackWrapper

logger = get_logger(__name__)


class BaseStackStore(ABC):
    """Base class for accessing data in CoalescenceML Repository and new Service."""

    def initialize(
        self,
        url: str,
        skip_default_stack: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> "BaseStackStore":
        """Initialize the store.
        Args:
            url: The URL of the store.
            skip_default_stack: If True, the creation of the default stack will
                be skipped.
            *args: Additional arguments to pass to the concrete store
                implementation.
            **kwargs: Additional keyword arguments to pass to the concrete
                store implementation.
        Returns:
            The initialized concrete store instance.
        """

        if not skip_default_stack and self.is_empty:

            logger.info("Initializing store...")
            self.register_default_stack()

        return self

    # Statics:

    @staticmethod
    @abstractmethod
    def get_path_from_url(url: str) -> Optional[Path]:
        """Get the path from a URL, if it points or is backed by a local file.
        Args:
            url: The URL to get the path from.
        Returns:
            The local path backed by the URL, or None if the URL is not backed
            by a local file or directory
        """

    @staticmethod
    @abstractmethod
    def get_local_url(path: str) -> str:
        """Get a local URL for a given local path.
        Args:
             path: the path string to build a URL out of.
        Returns:
            Url pointing to the path for the store type.
        """

    @staticmethod
    @abstractmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is valid."""

    # Public Interface:

    @property
    @abstractmethod
    def type(self) -> StoreType:
        """The type of stack store."""

    @property
    @abstractmethod
    def url(self) -> str:
        """Get the repository URL."""

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        """Check if the store is empty (no stacks are configured).
        The implementation of this method should check if the store is empty
        without having to load all the stacks from the persistent storage.
        """

    @abstractmethod
    def get_stack_configuration(
        self, name: str
    ) -> Dict[StackComponentFlavor, str]:
        """Fetches a stack configuration by name.
        Args:
            name: The name of the stack to fetch.
        Returns:
            Dict[StackComponentFlavor, str] for the requested stack name.
        Raises:
            KeyError: If no stack exists for the given name.
        """

    @property
    @abstractmethod
    def stack_configurations(self) -> Dict[str, Dict[StackComponentFlavor, str]]:
        """Configurations for all stacks registered in this stack store.
        Returns:
            Dictionary mapping stack names to Dict[StackComponentFlavor, str]'s
        """

    @abstractmethod
    def register_stack_component(
        self,
        component: StackComponentWrapper,
    ) -> None:
        """Register a stack component.
        Args:
            component: The component to register.
        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """

    @abstractmethod
    def deregister_stack(self, name: str) -> None:
        """Delete a stack from storage.
        Args:
            name: The name of the stack to be deleted.
        Raises:
            KeyError: If no stack exists for the given name.
        """

    # Private interface (must be implemented, not to be called by user):

    @abstractmethod
    def _create_stack(
        self, name: str, stack_configuration: Dict[StackComponentFlavor, str]
    ) -> None:
        """Add a stack to storage.
        Args:
            name: The name to save the stack as.
            stack_configuration: Dict[StackComponentFlavor, str] to persist.
        """

    @abstractmethod
    def _get_component_flavor_and_config(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> Tuple[str, bytes]:
        """Fetch the flavor and configuration for a stack component.
        Args:
            component_flavor: The type of the component to fetch.
            name: The name of the component to fetch.
        Returns:
            Pair of (flavor, configuration) for stack component, as string and
            base64-encoded yaml document, respectively
        Raises:
            KeyError: If no stack component exists for the given type and name.
        """

    @abstractmethod
    def _get_stack_component_names(
        self, component_flavor: StackComponentFlavor
    ) -> List[str]:
        """Get names of all registered stack components of a given type.
        Args:
            component_flavor: The type of the component to list names for.
        Returns:
            A list of names as strings.
        """

    @abstractmethod
    def _delete_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> None:
        """Remove a StackComponent from storage.
        Args:
            component_flavor: The type of component to delete.
            name: Then name of the component to delete.
        Raises:
            KeyError: If no component exists for given type and name.
        """

    # Common code (user facing):

    @property
    def stacks(self) -> List[StackWrapper]:
        """All stacks registered in this stack store."""
        return [
            self._stack_from_dict(name, conf)
            for name, conf in self.stack_configurations.items()
        ]

    def get_stack(self, name: str) -> StackWrapper:
        """Fetch a stack by name.
        Args:
            name: The name of the stack to retrieve.
        Returns:
            StackWrapper instance if the stack exists.
        Raises:
            KeyError: If no stack exists for the given name.
        """
        return self._stack_from_dict(name, self.get_stack_configuration(name))

    def register_stack(self, stack: StackWrapper) -> Dict[str, str]:
        """Register a stack and its components.
        If any of the stacks' components aren't registered in the stack store
        yet, this method will try to register them as well.
        Args:
            stack: The stack to register.
        Returns:
            metadata dict for telemetry or logging.
        Raises:
            StackExistsError: If a stack with the same name already exists.
            StackComponentExistsError: If a component of the stack wasn't
                registered and a different component with the same name
                already exists.
        """
        try:
            self.get_stack(stack.name)
        except KeyError:
            pass
        else:
            raise StackExistsError(
                f"Unable to register stack with name '{stack.name}': Found "
                f"existing stack with this name."
            )

        def __check_component(
            component: StackComponentWrapper,
        ) -> Tuple[StackComponentFlavor, str]:
            """Try to register a stack component, if it doesn't exist.
            Args:
                component: StackComponentWrapper to register.
            Returns:
                metadata key value pair for telemetry.
            Raises:
                StackComponentExistsError: If a component with same name exists.
            """
            try:
                existing_component = self.get_stack_component(
                    component_flavor=component.type, name=component.name
                )
                if existing_component.uuid != component.uuid:
                    raise StackComponentExistsError(
                        f"Unable to register one of the stacks components: "
                        f"A component of type '{component.type}' and name "
                        f"'{component.name}' already exists."
                    )
            except KeyError:
                self.register_stack_component(component)
            return component.type, component.name

        stack_configuration = {
            typ: name for typ, name in map(__check_component, stack.components)
        }
        metadata = {c.type.value: c.flavor for c in stack.components}
        self._create_stack(stack.name, stack_configuration)
        return metadata

    def get_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> StackComponentWrapper:
        """Get a registered stack component.
        Raises:
            KeyError: If no component with the requested type and name exists.
        """
        flavor, config = self._get_component_flavor_and_config(
            component_flavor, name=name
        )
        uuid = yaml.safe_load(base64.b64decode(config).decode())["uuid"]
        return StackComponentWrapper(
            type=component_flavor,
            flavor=flavor,
            name=name,
            uuid=uuid,
            config=config,
        )

    def get_stack_components(
        self, component_flavor: StackComponentFlavor
    ) -> List[StackComponentWrapper]:
        """Fetches all registered stack components of the given type.
        Args:
            component_flavor: StackComponentFlavor to list members of
        Returns:
            A list of StackComponentConfiguration instances.
        """
        return [
            self.get_stack_component(component_flavor=component_flavor, name=name)
            for name in self._get_stack_component_names(component_flavor)
        ]

    def deregister_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> None:
        """Deregisters a stack component.
        Args:
            component_flavor: The type of the component to deregister.
            name: The name of the component to deregister.
        Raises:
            ValueError: if trying to deregister a component that's part
                of a stack.
        """
        for stack_name, stack_config in self.stack_configurations.items():
            if stack_config.get(component_flavor) == name:
                raise ValueError(
                    f"Unable to deregister stack component (type: "
                    f"{component_flavor}, name: {name}) that is part of a "
                    f"registered stack (stack name: '{stack_name}')."
                )
        self._delete_stack_component(component_flavor, name=name)

    def register_default_stack(self) -> None:
        """Populates the store with the default Stack.
        The default stack contains a local orchestrator,
        a local artifact store and a local SQLite metadata store.
        """
        stack = Stack.default_local_stack()
        metadata = self.register_stack(StackWrapper.from_stack(stack))
        metadata["store_type"] = self.type.value
        track_event(AnalyticsEvent.REGISTERED_STACK, metadata=metadata)

    # Common code (internal implementations, private):

    def _stack_from_dict(
        self, name: str, stack_configuration: Dict[StackComponentFlavor, str]
    ) -> StackWrapper:
        """Build a StackWrapper from stored configurations"""
        stack_components = [
            self.get_stack_component(
                component_flavor=component_flavor, name=component_name
            )
            for component_flavor, component_name in stack_configuration.items()
        ]
        return StackWrapper(name=name, components=stack_components)