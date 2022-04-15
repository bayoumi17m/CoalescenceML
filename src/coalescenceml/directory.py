import base64
import json
import os
from abc import ABCMeta
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

import yaml
from pydantic import BaseModel

from coalescenceml.config.base_config import BaseConfiguration
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.config.profile_config import ProfileConfiguration
from coalescenceml.constants import (
    DIRECTORY_DIRECTORY_NAME,
    ENV_COML_DIRECTORY_PATH,
)
from coalescenceml.enums import DirectoryStoreFlavor, StackComponentFlavor
from coalescenceml.environment import Environment
from coalescenceml.exceptions import (
    ForbiddenDirectoryAccessError,
    InitializationException,
)
from coalescenceml.io import fileio, utils
from coalescenceml.logger import get_logger
from coalescenceml.post_execution import PipelineView
from coalescenceml.stack import Stack, StackComponent
from coalescenceml.stack_store import (
    BaseStackStore,
    LocalStackStore,
    SqlStackStore,
)
from coalescenceml.stack_store.model import StackComponentWrapper, StackWrapper
from coalescenceml.utils import yaml_utils


logger = get_logger(__name__)


class DirectoryConfiguration(BaseModel):
    """Pydantic object used for serializing directory configuration options.
    Attributes:
        active_profile_name: The name of the active profile.
        active_stack_name: Optional name of the active stack.
    """

    active_profile_name: Optional[str]
    active_stack_name: Optional[str]

    class Config:
        """Pydantic configuration class."""

        # Validate attributes when assigning them. We need to set this in order
        # to have a mix of mutable and immutable attributes
        validate_assignment = True
        # Ignore extra attributes from configs of previous COML versions
        extra = "ignore"
        # all attributes with leading underscore are private and therefore
        # are mutable and not included in serialization
        underscore_attrs_are_private = True


class DirectoryMetaClass(ABCMeta):
    """Directory singleton metaclass.
    This metaclass is used to enforce a singleton instance of the Directory
    class with the following additional properties:
    * the singleton Directory instance is created on first access to reflect
    the currently active global configuration profile.
    * the Directory mustn't be accessed from within pipeline steps
    """

    def __init__(cls, *args: Any, **kwargs: Any) -> None:
        """Initialize the Directory class."""
        super().__init__(*args, **kwargs)
        cls._global_directory: Optional["Directory"] = None

    def __call__(cls, *args: Any, **kwargs: Any) -> "Directory":
        """Create or return the global Directory instance.
        If the Directory constructor is called with custom arguments,
        the singleton functionality of the metaclass is bypassed: a new
        Directory instance is created and returned immediately and without
        saving it as the global Directory singleton.
        Raises:
            ForbiddenDirectoryAccessError: If trying to create a `Directory`
                instance while a COML step is being executed.
        """

        # `skip_directory_check` is a special kwarg that can be passed to
        # the Directory constructor to bypass the check that prevents the
        # Directory instance from being accessed from within pipeline steps.
        if not kwargs.pop("skip_directory_check", False):
            if Environment().step_is_running:
                raise ForbiddenDirectoryAccessError(
                    "Unable to access directory during step execution. If you "
                    "require access to the artifact or metadata store, please "
                    "use a `StepContext` inside your step instead."
                )

        if args or kwargs:
            return cast("Directory", super().__call__(*args, **kwargs))

        if not cls._global_directory:
            cls._global_directory = cast(
                "Directory", super().__call__(*args, **kwargs)
            )

        return cls._global_directory


class Directory(BaseConfiguration, metaclass=DirectoryMetaClass):
    """COML directory class.
    The COML directory manages configuration options for COML stacks as well
    as their components.
    """

    def __init__(
        self,
        root: Optional[Path] = None,
        profile: Optional[ProfileConfiguration] = None,
    ) -> None:
        """Initializes the global directory instance.
        Directory is a singleton class: only one instance can exist. Calling
        this constructor multiple times will always yield the same instance (see
        the exception below).
        The `root` and `profile` arguments are only meant for internal use
        and testing purposes. User code must never pass them to the constructor.
        When a custom `root` or `profile` value is passed, an anonymous
        Directory instance is created and returned independently of the
        Directory singleton and that will have no effect as far as the rest of
        the COML core code is concerned.
        Instead of creating a new Directory instance to reflect a different
        profile or directory root:
          * to change the active profile in the global Directory,
          call `Directory().activate_profile(<new-profile>)`.
          * to change the active root in the global Directory,
          call `Directory().activate_root(<new-root>)`.
        Args:
            root: (internal use) custom root directory for the directory. If
                no path is given, the directory root is determined using the
                environment variable `COML_DIRECTORY_PATH` (if set) and by
                recursively searching in the parent directories of the
                current working directory. Only used to initialize new
                repositories internally.
            profile: (internal use) custom configuration profile to use for the
                directory. If not provided, the active profile is determined
                from the loaded directory configuration. If no directory
                configuration is found (i.e. directory root is not
                initialized), the default global profile is used. Only used to
                initialize new profiles internally.
        """

        self._root: Optional[Path] = None
        self._profile: Optional[ProfileConfiguration] = None
        self.__config: Optional[DirectoryConfiguration] = None

        # The directory constructor is called with a custom profile only when
        # the profile needs to be initialized, in which case all matters related
        # to directory initialization, like the directory active root and the
        # directory configuration stored there are ignored
        if profile:
            # calling this will initialize the store and create the default
            # stack configuration, if missing
            self._set_active_profile(profile)
            return

        self._set_active_root(root)

    @classmethod
    def get_instance(cls) -> Optional["Directory"]:
        """Return the Directory singleton instance.
        Returns:
            The Directory singleton instance or None, if the Directory hasn't
            been initialized yet.
        """
        return cls._global_directory

    @classmethod
    def _reset_instance(cls, repo: Optional["Directory"] = None) -> None:
        """Reset the Directory singleton instance.
        This method is only meant for internal use and testing purposes.
        Args:
            repo: The Directory instance to set as the global singleton.
                If None, the global Directory singleton is reset to an empty
                value.
        """
        cls._global_directory = repo

    def get_active_root(self) -> str:
        """Get the currently active path set at the directory root."""
        if not self._root:
            logger.info("Running without an active directory root.")
            # TODO: Raise error?
        else:
            logger.debug("Using directory root %s.", self._root)
            return self._root

    def _set_active_root(self, root: Optional[Path] = None) -> None:
        """Set the supplied path as the directory root.
        If a directory configuration is found at the given path or the
        path, it is loaded and used to initialize the directory and its
        active profile. If no directory configuration is found, the
        global configuration is used instead (e.g. to manage the active
        profile and active stack).
        Args:
            root: The path to set as the active directory root. If not set,
                the directory root is determined using the environment
                variable `COML_DIRECTORY_PATH` (if set) and by recursively
                searching in the parent directories of the current working
                directory.
        """

        self._root = self.find_directory(root, enable_warnings=True)

        global_cfg = GlobalConfiguration()
        new_profile = self._profile

        if not self._root:
            logger.info("Running without an active directory root.")
        else:
            logger.debug("Using directory root %s.", self._root)
            self.__config = self._load_config()

            if self.__config and self.__config.active_profile_name:
                new_profile = global_cfg.get_profile(
                    self.__config.active_profile_name
                )

        # fall back to the global active profile if one cannot be determined
        # from the directory configuration
        new_profile = new_profile or global_cfg.active_profile

        if not new_profile:
            # this should theoretically never happen, because there is always
            # a globally active profile, but we need to be prepared for it
            raise RuntimeError(
                "No active configuration profile found. Please set the active "
                "profile in the global configuration by running `coml profile "
                "set <profile-name>`."
            )

        if new_profile != self._profile:
            logger.debug(
                "Activating configuration profile %s.", new_profile.name
            )
            self._set_active_profile(new_profile)

        # Sanitize the directory configuration to reflect the new active
        # profile
        self._sanitize_config()

    def _set_active_profile(self, profile: ProfileConfiguration) -> None:
        """Set the supplied configuration profile as the active profile for
        this directory.
        This method initializes the directory store associated with the
        supplied profile and also initializes it with the default stack
        configuration, if no other stacks are configured.
        Args:
            profile: configuration profile to set as active.
        """
        self._profile = profile
        self.stack_store: BaseStackStore = self.create_store(profile)

        # Sanitize the directory configuration to reflect the active
        # profile and its store contents
        self._sanitize_config()

    def _config_path(self) -> Optional[str]:
        """Path to the directory configuration file.
        Returns:
            Path to the directory configuration file or None if the directory
            root has not been initialized yet.
        """
        if not self.config_directory:
            return None
        return str(self.config_directory / "config.yaml")

    def _sanitize_config(self) -> None:
        """Sanitize and save the directory configuration.
        This method is called to ensure that the directory configuration
        doesn't contain outdated information, such as an active profile or an
        active stack that no longer exists.
        Raises:
            RuntimeError: If the directory configuration doesn't contain a
            valid active stack and a new active stack cannot be automatically
            determined based on the active profile and available stacks.
        """
        if not self.__config:
            return

        global_cfg = GlobalConfiguration()

        # Sanitize the directory active profile
        if self.__config.active_profile_name != self.active_profile_name:
            if (
                self.__config.active_profile_name
                and not global_cfg.has_profile(
                    self.__config.active_profile_name
                )
            ):
                logger.warning(
                    "Profile `%s` not found. Switching directory to the "
                    "global active profile `%s`",
                    self.__config.active_profile_name,
                    self.active_profile_name,
                )
            # reset the active stack when switching to a different profile
            self.__config.active_stack_name = None
            self.__config.active_profile_name = self.active_profile_name

        # As a backup for the active stack, use the profile's default stack
        # or to the first stack in the directory
        backup_stack_name = self.active_profile.active_stack
        if not backup_stack_name:
            stacks = self.stack_store.stacks
            if stacks:
                backup_stack_name = stacks[0].name

        # Sanitize the directory active stack
        if not self.__config.active_stack_name:
            self.__config.active_stack_name = backup_stack_name

        if not self.__config.active_stack_name:
            raise RuntimeError(
                "Could not determine active stack. Please set the active stack "
                "by running `coml stack set <stack-name>`."
            )

        # Ensure that the directory active stack is still valid
        try:
            self.stack_store.get_stack(self.__config.active_stack_name)
        except KeyError:
            logger.warning(
                "Stack `%s` not found. Switching the directory active stack "
                "to `%s`",
                self.__config.active_stack_name,
                backup_stack_name,
            )
            self.__config.active_stack_name = backup_stack_name

        self._write_config()

    def _load_config(self) -> Optional[DirectoryConfiguration]:
        """Loads the directory configuration from disk, if the directory has
        an active root and the configuration file exists. If the configuration
        file doesn't exist, an empty configuration is returned.
        If the directory doesn't have an active root, no directory
        configuration is used and the active profile configuration takes
        precedence.
        Returns:
            Loaded directory configuration or None if the directory does not
            have an active root.
        """

        config_path = self._config_path()
        if not config_path:
            return None

        # load the directory configuration file if it exists, otherwise use
        # an empty configuration as default
        if fileio.exists(config_path):
            logger.debug(f"Loading directory configuration from {config_path}.")

            config_dict = yaml_utils.read_yaml(config_path)
            config = DirectoryConfiguration.parse_obj(config_dict)

            return config

        logger.debug(
            "No directory configuration file found, creating default "
            "configuration."
        )
        return DirectoryConfiguration()

    def _write_config(self) -> None:
        """Writes the directory configuration to disk, if the directory has
        been initialized."""
        config_path = self._config_path()
        if not config_path or not self.__config:
            return
        config_dict = json.loads(self.__config.json())
        yaml_utils.write_yaml(config_path, config_dict)

    @staticmethod
    def get_store_class(
        type: DirectoryStoreFlavor,
    ) -> Optional[Type[BaseStackStore]]:
        """Returns the class of the given store type."""
        return {
            DirectoryStoreFlavor.LOCAL: LocalStackStore,
            DirectoryStoreFlavor.SQL: SqlStackStore,
        }.get(type)

    @staticmethod
    def create_store(
        profile: ProfileConfiguration, skip_default_stack: bool = False
    ) -> BaseStackStore:
        """Create the directory persistence back-end store from a configuration
        profile.
        If the configuration profile doesn't specify all necessary configuration
        options (e.g. the type or URL), a default configuration will be used.
        Args:
            profile: The configuration profile to use for persisting the
                directory information.
            skip_default_stack: If True, the creation of the default stack in
                the store will be skipped.
        Returns:
            The initialized directory store.
        """
        if not profile.store_type:
            raise RuntimeError(
                f"Store type not configured in profile {profile.name}"
            )

        store_class = Directory.get_store_class(profile.store_type)
        if not store_class:
            raise RuntimeError(
                f"No store implementation found for store type "
                f"`{profile.store_type}`."
            )

        if not profile.store_url:
            profile.store_url = store_class.get_local_url(
                profile.config_directory
            )

        if store_class.is_valid_url(profile.store_url):
            store = store_class()
            store.initialize(
                url=profile.store_url, skip_default_stack=skip_default_stack
            )
            return store

        raise ValueError(
            f"Invalid URL for store type `{profile.store_type.value}`: "
            f"{profile.store_url}"
        )

    @staticmethod
    def initialize(
        root: Optional[Path] = None,
    ) -> None:
        """Initializes a new COML directory at the given path.
        Args:
            root: The root directory where the directory should be created.
                If None, the current working directory is used.
        Raises:
            InitializationException: If the root directory already contains a
                COML directory.
        """
        root = root or Path.cwd()
        logger.debug("Initializing new directory at path %s.", root)
        if Directory.is_directory_directory(root):
            raise InitializationException(
                f"Found existing COML directory at path '{root}'."
            )

        config_directory = str(root / DIRECTORY_DIRECTORY_NAME)
        utils.create_dir_recursive_if_not_exists(config_directory)
        # Initialize the directory configuration at the custom path
        Directory(root=root)

    def initialize_store(self) -> None:
        """Initializes the COML store for the directory.
        The store will contain a single stack with a local orchestrator,
        a local artifact store and a local SQLite metadata store.
        """

        # register and activate a local stack
        stack = Stack.default_local_stack()
        self.register_stack(stack)
        self.activate_stack(stack.name)

    @property
    def root(self) -> Optional[Path]:
        """The root directory of this directory.
        Returns:
            The root directory of this directory, or None, if the directory
            has not been initialized.
        """
        return self._root

    @property
    def config_directory(self) -> Optional[Path]:
        """The configuration directory of this directory.
        Returns:
            The configuration directory of this directory, or None, if the
            directory doesn't have an active root.
        """
        if not self.root:
            return None
        return self.root / DIRECTORY_DIRECTORY_NAME

    def activate_root(self, root: Optional[Path] = None) -> None:
        """Set the active directory root directory.
        Args:
            root: The path to set as the active directory root. If not set,
                the directory root is determined using the environment
                variable `COML_DIRECTORY_PATH` (if set) and by recursively
                searching in the parent directories of the current working
                directory.
        """
        self._set_active_root(root)

    def activate_profile(self, profile_name: str) -> None:
        """Set a profile as the active profile for the directory.
        Args:
            profile_name: name of the profile to add
        Raises:
            KeyError: If the profile with the given name does not exist.
        """
        global_cfg = GlobalConfiguration()
        profile = global_cfg.get_profile(profile_name)
        if not profile:
            raise KeyError(f"Profile '{profile_name}' not found.")
        if profile is self._profile:
            # profile is already active
            return

        self._set_active_profile(profile)

        # set the active profile in the global configuration if the directory
        # doesn't have a root configured (i.e. if a directory root has not been
        # initialized)
        if not self.root:
            global_cfg.activate_profile(profile_name)

    @property
    def active_profile(self) -> ProfileConfiguration:
        """Return the profile set as active for the directory.
        Returns:
            The active profile.
        Raises:
            RuntimeError: If no profile is set as active.
        """
        if not self._profile:
            # this should theoretically never happen, because there is always
            # a globally active profile, but we need to be prepared for it
            raise RuntimeError(
                "No active configuration profile found. Please set the active "
                "profile in the global configuration by running `coml profile "
                "set <profile-name>`."
            )

        return self._profile

    @property
    def active_profile_name(self) -> str:
        """Return the name of the profile set as active for the directory.
        Returns:
            The active profile name.
        Raises:
            RuntimeError: If no profile is set as active.
        """
        return self.active_profile.name

    @property
    def stacks(self) -> List[Stack]:
        """All stacks registered in this directory."""
        return [self._stack_from_wrapper(s) for s in self.stack_store.stacks]

    @property
    def stack_configurations(
        self,
    ) -> Dict[str, Dict[StackComponentFlavor, str]]:
        """Configuration dicts for all stacks registered in this directory.
        This property is intended as a quick way to get information about the
        components of the registered stacks without loading all installed
        integrations. The contained stack configurations might be invalid if
        they were modified by hand, to ensure you get valid stacks use
        `repo.stacks()` instead.
        Modifying the contents of the returned dictionary does not actually
        register/deregister stacks, use `repo.register_stack(...)` or
        `repo.deregister_stack(...)` instead.
        """
        return self.stack_store.stack_configurations

    @property
    def active_stack(self) -> Stack:
        """The active stack for this directory.
        Raises:
            RuntimeError: If no active stack name is configured.
            KeyError: If no stack was found for the configured name or one
                of the stack components is not registered.
        """
        return self.get_stack(name=self.active_stack_name)

    @property
    def active_stack_name(self) -> str:
        """The name of the active stack for this directory.
        If no active stack is configured for the directory, or if the
        directory does not have an active root, the active stack from the
        associated or global profile is used instead.
        Raises:
            RuntimeError: If no active stack name is set neither in the
            directory configuration nor in the associated profile.
        """
        stack_name = None
        if self.__config:
            stack_name = self.__config.active_stack_name

        if not stack_name:
            stack_name = self.active_profile.active_stack

        if not stack_name:
            raise RuntimeError(
                "No active stack is configured for the directory. Run "
                "`coml stack set STACK_NAME` to update the active stack."
            )

        return stack_name

    def activate_stack(self, name: str) -> None:
        """Activates the stack for the given name.
        Args:
            name: Name of the stack to activate.
        Raises:
            KeyError: If no stack exists for the given name.
        """
        self.stack_store.get_stack_configuration(name)  # raises KeyError
        if self.__config:
            self.__config.active_stack_name = name
            self._write_config()

        # set the active stack globally in the active profile only if the
        # directory doesn't have a root configured (i.e. directory root hasn't
        # been initialized) or if no active stack has been set for it yet
        if not self.root or not self.active_profile.active_stack:
            self.active_profile.activate_stack(name)

    def get_stack(self, name: str) -> Stack:
        """Fetches a stack.
        Args:
            name: The name of the stack to fetch.
        Raises:
            KeyError: If no stack exists for the given name or one of the
                stacks components is not registered.
        """
        return self._stack_from_wrapper(self.stack_store.get_stack(name))

    def register_stack(self, stack: Stack) -> None:
        """Registers a stack and it's components.
        If any of the stacks' components aren't registered in the directory
        yet, this method will try to register them as well.
        Args:
            stack: The stack to register.
        Raises:
            StackExistsError: If a stack with the same name already exists.
            StackComponentExistsError: If a component of the stack wasn't
                registered and a different component with the same name
                already exists.
        """
        metadata = self.stack_store.register_stack(
            StackWrapper.from_stack(stack)
        )
        metadata["store_type"] = self.active_profile.store_type.value

    def deregister_stack(self, name: str) -> None:
        """Deregisters a stack.
        Args:
            name: The name of the stack to deregister.
        Raises:
            ValueError: If the stack is the currently active stack for this
                directory.
        """
        if name == self.active_stack_name:
            raise ValueError(f"Unable to deregister active stack '{name}'.")
        try:
            self.stack_store.deregister_stack(name)
            logger.info("Deregistered stack with name '%s'.", name)
        except KeyError:
            logger.warning(
                "Unable to deregister stack with name '%s': No stack  "
                "with this name could be found.",
                name,
            )

    def get_stack_components(
        self, component_flavor: StackComponentFlavor
    ) -> List[StackComponent]:
        """Fetches all registered stack components of the given type."""
        return [
            self._component_from_wrapper(c)
            for c in self.stack_store.get_stack_components(component_flavor)
        ]

    def get_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> StackComponent:
        """Fetches a registered stack component.
        Args:
            component_flavor: The type of the component to fetch.
            name: The name of the component to fetch.
        Raises:
            KeyError: If no stack component exists for the given type and name.
        """
        logger.debug(
            "Fetching stack component of type '%s' with name '%s'.",
            component_flavor.value,
            name,
        )
        return self._component_from_wrapper(
            self.stack_store.get_stack_component(component_flavor, name=name)
        )

    def register_stack_component(
        self,
        component: StackComponent,
    ) -> None:
        """Registers a stack component.
        Args:
            component: The component to register.
        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """
        self.stack_store.register_stack_component(
            StackComponentWrapper.from_component(component)
        )
        analytics_metadata = {
            "type": component.TYPE.value,
            "flavor": component.FLAVOR,
        }

    def deregister_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> None:
        """Deregisters a stack component.
        Args:
            component_flavor: The type of the component to deregister.
            name: The name of the component to deregister.
        """
        try:
            self.stack_store.deregister_stack_component(
                component_flavor, name=name
            )
            logger.info(
                "Deregistered stack component (type: %s) with name '%s'.",
                component_flavor.value,
                name,
            )
        except KeyError:
            logger.warning(
                "Unable to deregister stack component (type: %s) with name "
                "'%s': No stack component with this name could be found.",
                component_flavor.value,
                name,
            )

    def get_pipelines(
        self, stack_name: Optional[str] = None
    ) -> List[PipelineView]:
        """Fetches post-execution pipeline views.
        Args:
            stack_name: If specified, pipelines in the metadata store of the
                given stack are returned. Otherwise, pipelines in the metadata
                store of the currently active stack are returned.
        Returns:
            A list of post-execution pipeline views.
        Raises:
            RuntimeError: If no stack name is specified and no active stack name
                is configured.
            KeyError: If no stack with the given name exists.
        """
        stack_name = stack_name or self.active_stack_name
        if not stack_name:
            raise RuntimeError(
                "No active stack is configured for the directory. Run "
                "`coml stack set STACK_NAME` to update the active stack."
            )
        metadata_store = self.get_stack(stack_name).metadata_store
        return metadata_store.get_pipelines()

    def get_pipeline(
        self, pipeline_name: str, stack_name: Optional[str] = None
    ) -> Optional[PipelineView]:
        """Fetches a post-execution pipeline view.
        Args:
            pipeline_name: Name of the pipeline.
            stack_name: If specified, pipelines in the metadata store of the
                given stack are returned. Otherwise, pipelines in the metadata
                store of the currently active stack are returned.
        Returns:
            A post-execution pipeline view for the given name or `None` if
            it doesn't exist.
        Raises:
            RuntimeError: If no stack name is specified and no active stack name
                is configured.
            KeyError: If no stack with the given name exists.
        """
        stack_name = stack_name or self.active_stack_name
        if not stack_name:
            raise RuntimeError(
                "No active stack is configured for the directory. Run "
                "`coml stack set STACK_NAME` to update the active stack."
            )
        metadata_store = self.get_stack(stack_name).metadata_store
        return metadata_store.get_pipeline(pipeline_name)

    @staticmethod
    def is_directory_directory(path: Path) -> bool:
        """Checks whether a COML directory exists at the given path."""
        config_dir = path / DIRECTORY_DIRECTORY_NAME
        return fileio.isdir(str(config_dir))

    @staticmethod
    def find_directory(
        path: Optional[Path] = None, enable_warnings: bool = False
    ) -> Optional[Path]:
        """Search for a COML directory directory.
        Args:
            path: Optional path to look for the directory. If no path is
                given, this function tries to find the directory using the
                environment variable `COML_DIRECTORY_PATH` (if set) and
                recursively searching in the parent directories of the current
                working directory.
            enable_warnings: If `True`, warnings are printed if the directory
                root cannot be found.
        Returns:
            Absolute path to a COML directory directory or None if no
            directory directory was found.
        """
        if not path:
            # try to get path from the environment variable
            env_var_path = os.getenv(ENV_COML_DIRECTORY_PATH)
            if env_var_path:
                path = Path(env_var_path)

        if path:
            # explicit path via parameter or environment variable, don't search
            # parent directories
            search_parent_directories = False
            warning_message = (
                f"Unable to find CoalescenceML directory at path '{path}'. Make sure "
                f"to create a CoalescenceML directory by calling `coml init` when "
                f"specifying an explicit directory path in code or via the "
                f"environment variable '{ENV_COML_DIRECTORY_PATH}'."
            )
        else:
            # try to find the repo in the parent directories of the current
            # working directory
            path = Path.cwd()
            search_parent_directories = True
            warning_message = (
                f"Unable to find COML directory in your current working "
                f"directory ({path}) or any parent directories. If you "
                f"want to use an existing directory which is in a different "
                f"location, set the environment variable "
                f"'{ENV_COML_DIRECTORY_PATH}'. If you want to create a new "
                f"directory, run `coml init`."
            )

        def _find_repo_helper(path_: Path) -> Optional[Path]:
            """Helper function to recursively search parent directories for a
            COML directory."""
            if Directory.is_directory_directory(path_):
                return path_

            if not search_parent_directories or utils.is_root(str(path_)):
                return None

            return _find_repo_helper(path_.parent)

        repo_path = _find_repo_helper(path)

        if repo_path:
            return repo_path.resolve()
        if enable_warnings:
            logger.warning(warning_message)
        return None

    def _component_from_wrapper(
        self, wrapper: StackComponentWrapper
    ) -> StackComponent:
        """Instantiate a StackComponent from the Configuration."""
        from coalescenceml.stack.stack_component_class_registry import (
            StackComponentClassRegistry,
        )

        component_class = StackComponentClassRegistry.get_class(
            component_type=wrapper.type, component_flavor=wrapper.flavor
        )
        component_config = yaml.safe_load(
            base64.b64decode(wrapper.config).decode()
        )
        return component_class.parse_obj(component_config)

    def _stack_from_wrapper(self, wrapper: StackWrapper) -> Stack:
        """Instantiate a Stack from the serializable Wrapper."""
        stack_components = {}
        for component_wrapper in wrapper.components:
            component_flavor = component_wrapper.type
            component = self._component_from_wrapper(component_wrapper)
            stack_components[component_flavor] = component

        return Stack.from_components(
            name=wrapper.name, components=stack_components
        )
