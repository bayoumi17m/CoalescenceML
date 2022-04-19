import base64
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from coalescenceml.enums import DirectoryStoreFlavor, StackComponentFlavor
from coalescenceml.io import fileio, utils
from coalescenceml.logger import get_logger
from coalescenceml.stack.exceptions import StackComponentExistsError
from coalescenceml.stack_store import BaseStackStore
from coalescenceml.stack_store.model import (
    StackComponentWrapper,
    StackStoreModel,
)
from coalescenceml.utils import yaml_utils


logger = get_logger(__name__)


class LocalStackStore(BaseStackStore):
    def initialize(
        self,
        url: str,
        *args: Any,
        stack_data: Optional[StackStoreModel] = None,
        **kwargs: Any,
    ) -> "LocalStackStore":
        """Initializes a local stack store instance.
        Args:
            url: URL of local directory of the repository to use for
                stack storage.
            stack_data: optional stack data store object to pre-populate the
                stack store with.
            args: additional positional arguments (ignored).
            kwargs: additional keyword arguments (ignored).
        Returns:
            The initialized stack store instance.
        """
        if not self.is_valid_url(url):
            raise ValueError(f"Invalid URL for local store: {url}")

        self._root = self.get_path_from_url(url)
        self._url = f"file://{self._root}"
        utils.create_dir_recursive_if_not_exists(str(self._root))

        if stack_data is not None:
            self.__store = stack_data
            self._write_store()
        elif fileio.exists(self._store_path()):
            config_dict = yaml_utils.read_yaml(self._store_path())
            self.__store = StackStoreModel.parse_obj(config_dict)
        else:
            self.__store = StackStoreModel.empty_store()
            self._write_store()

        super().initialize(url, *args, **kwargs)
        return self

    # Public interface implementations:

    @property
    def type(self) -> DirectoryStoreFlavor:
        """The type of stack store."""
        return DirectoryStoreFlavor.LOCAL

    @property
    def url(self) -> str:
        """URL of the repository."""
        return self._url

    @staticmethod
    def get_path_from_url(url: str) -> Optional[Path]:
        """Get the path from a URL.
        Args:
            url: The URL to get the path from.
        Returns:
            The path from the URL.
        """
        if not LocalStackStore.is_valid_url(url):
            raise ValueError(f"Invalid URL for local store: {url}")
        url = url.replace("file://", "")
        return Path(url)

    @staticmethod
    def get_local_url(path: str) -> str:
        """Get a local URL for a given local path."""
        return f"file://{path}"

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is a valid local path."""
        scheme = re.search("^([a-z0-9]+://)", url)
        return not scheme or scheme.group() == "file://"

    @property
    def is_empty(self) -> bool:
        """Check if the stack store is empty."""
        return len(self.__store.stacks) == 0

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
        logger.debug("Fetching stack with name '%s'.", name)
        if name not in self.__store.stacks:
            raise KeyError(
                f"Unable to find stack with name '{name}'. Available names: "
                f"{set(self.__store.stacks)}."
            )

        return self.__store.stacks[name]

    @property
    def stack_configurations(
        self,
    ) -> Dict[str, Dict[StackComponentFlavor, str]]:
        """Configuration for all stacks registered in this stack store.
        Returns:
            Dictionary mapping stack names to Dict[StackComponentFlavor, str]
        """
        return self.__store.stacks.copy()

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
        components = self.__store.stack_components[component.type]
        if component.name in components:
            raise StackComponentExistsError(
                f"Unable to register stack component (type: {component.type}) "
                f"with name '{component.name}': Found existing stack component "
                f"with this name."
            )

        # write the component configuration file
        component_config_path = self._get_stack_component_config_path(
            component_flavor=component.type, name=component.name
        )
        utils.create_dir_recursive_if_not_exists(
            os.path.dirname(component_config_path)
        )
        utils.write_file_contents_as_string(
            component_config_path,
            base64.b64decode(component.config).decode(),
        )

        # add the component to the stack store dict and write it to disk
        components[component.name] = component.flavor
        self._write_store()
        logger.info(
            "Registered stack component with type '%s' and name '%s'.",
            component.type,
            component.name,
        )

    def deregister_stack(self, name: str) -> None:
        """Remove a stack from storage.
        Args:
            name: The name of the stack to be deleted.
        Raises:
            KeyError: If no stack exists for the given name.
        """
        del self.__store.stacks[name]
        self._write_store()

    # Private interface implementations:

    def _create_stack(
        self, name: str, stack_configuration: Dict[StackComponentFlavor, str]
    ) -> None:
        """Add a stack to storage.
        Args:
            name: The name to save the stack as.
            stack_configuration: Dict[StackComponentFlavor, str] to persist.
        """
        self.__store.stacks[name] = stack_configuration
        self._write_store()
        logger.info("Registered stack with name '%s'.", name)

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
        components: Dict[str, str] = self.__store.stack_components[
            component_flavor
        ]
        if name not in components:
            raise KeyError(
                f"Unable to find stack component (type: {component_flavor}) "
                f"with name '{name}'. Available names: {set(components)}."
            )

        component_config_path = self._get_stack_component_config_path(
            component_flavor=component_flavor, name=name
        )
        flavor = components[name]
        config = base64.b64encode(
            utils.read_file_contents_as_string(component_config_path).encode()
        )
        return flavor, config

    def _get_stack_component_names(
        self, component_flavor: StackComponentFlavor
    ) -> List[str]:
        """Get names of all registered stack components of a given type."""
        return list(self.__store.stack_components[component_flavor])

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
        component_config_path = self._get_stack_component_config_path(
            component_flavor=component_flavor, name=name
        )

        if fileio.exists(component_config_path):
            fileio.remove(component_config_path)

        components = self.__store.stack_components[component_flavor]
        del components[name]
        self._write_store()

    # Implementation-specific internal methods:

    @property
    def root(self) -> Path:
        """The root directory of the stack store."""
        if not self._root:
            raise RuntimeError(
                "Local stack store has not been initialized. Call `initialize` "
                "before using the store."
            )
        return self._root

    def _get_stack_component_config_path(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> str:
        """Path to the configuration file of a stack component."""
        path = self.root / component_flavor / f"{name}.yaml"
        return str(path)

    def _store_path(self) -> str:
        """Path to the stack store yaml file."""
        return str(self.root / "stacks.yaml")

    def _write_store(self) -> None:
        """Writes the stack store yaml file."""
        config_dict = json.loads(self.__store.json())
        yaml_utils.write_yaml(self._store_path(), config_dict)
