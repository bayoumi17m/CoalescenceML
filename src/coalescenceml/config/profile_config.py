import os
from typing import TYPE_CHECKING, Any, Optional

import requests
from pydantic import BaseModel, Field

from coalescenceml.constants import ENV_COML_DEFAULT_STORE_TYPE
from coalescenceml.enums import DirectoryStoreFlavor
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger

from coalescenceml.config.global_config import GlobalConfiguration


logger = get_logger(__name__)


DEFAULT_PROFILE_NAME = "default"


def get_default_store_type() -> DirectoryStoreFlavor:
    """Return the default store type.
    The default store type can be set via the environment variable
    COML_DEFAULT_STORE_TYPE. If this variable is not set, the default
    store type is set to 'LOCAL'.
    NOTE: this is a global function instead of a default
    `ProfileConfiguration.store_type` value because it makes it easier to mock
    in the unit tests.
    Returns:
        The default store type.
    """
    store_type = os.getenv(ENV_COML_DEFAULT_STORE_TYPE)
    if store_type and store_type in DirectoryStoreFlavor.values():
        return DirectoryStoreFlavor(store_type)
    return DirectoryStoreFlavor.LOCAL


class ProfileConfiguration(BaseModel):
    """Stores configuration profile options.
    Attributes:
        name: Name of the profile.
        store_url: URL pointing to the CoalescenceML store backend.
        store_type: Type of the store backend.
        active_stack: Optional name of the active stack.
        _config: global configuration to which this profile belongs.
    """

    name: str
    store_url: Optional[str]
    store_type: StoreType = Field(default_factory=get_default_store_type)
    active_stack: Optional[str]
    _config: Optional["GlobalConfiguration"]

    def __init__(
        self, config: Optional[GlobalConfiguration] = None, **kwargs: Any
    ) -> None:
        """Initializes a ProfileConfiguration object.
        Args:
            config: global configuration to which this profile belongs. When not
                specified, the default global configuration path is used.
            **kwargs: additional keyword arguments are passed to the
                BaseModel constructor.
        """
        self._config = config
        super().__init__(**kwargs)

    @property
    def config_directory(self) -> str:
        """Directory where the profile configuration is stored."""
        return os.path.join(
            self.global_config.config_directory, "profiles", self.name
        )

    def initialize(self) -> None:
        """Initialize the profile."""

        # import here to avoid circular dependency
        from coalescenceml.directory import Directory

        logger.info("Initializing profile `%s`...", self.name)

        # Create and initialize the profile using a special directory instance.
        # This also validates and updates the store URL configuration and
        # creates all necessary resources (e.g. paths, initial DB, default
        # stacks).
        repo = Directory(profile=self)

        if not self.active_stack:
            try:
                stacks = repo.stacks
            except requests.exceptions.ConnectionError:
                stacks = None
            if stacks:
                self.active_stack = stacks[0].name

    def cleanup(self) -> None:
        """Cleanup the profile directory."""
        if fileio.isdir(self.config_directory):
            fileio.rmtree(self.config_directory)

    @property
    def global_config(self) -> GlobalConfiguration:
        """Return the global configuration to which this profile belongs."""
        return self._config or GlobalConfiguration()

    def activate_stack(self, stack_name: str) -> None:
        """Set the active stack for the profile.
        Args:
            stack_name: name of the stack to activate
        """
        self.active_stack = stack_name
        self.global_config._write_config()

    class Config:
        """Pydantic configuration class."""

        # Validate attributes when assigning them. We need to set this in order
        # to have a mix of mutable and immutable attributes
        validate_assignment = True
        # Ignore extra attributes from configs of previous CoalescenceML versions
        extra = "ignore"
        # all attributes with leading underscore are private and therefore
        # are mutable and not included in serialization
        underscore_attrs_are_private = True