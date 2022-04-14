from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from coalescenceml.config.profile_config import ProfileConfiguration


class BaseConfiguration(ABC):
    """Base class for global configuration management.
    This class defines the common interface related to profile and stack
    management that all global configuration classes must implement.
    Both the GlobalConfiguration and Repository classes implement this class,
    since they share similarities concerning the management of active profiles
    and stacks.
    """

    @abstractmethod
    def activate_profile(self, profile_name: str) -> None:
        """Set the active profile
        Args:
            profile_name: The name of the profile to set as active.
        Raises:
            KeyError: If the profile with the given name does not exist.
        """

    @property
    @abstractmethod
    def active_profile(self) -> Optional[ProfileConfiguration]:
        """Return the profile set as active for the repository.
        Returns:
            The active profile or None, if no active profile is set.
        """

    @property
    @abstractmethod
    def active_profile_name(self) -> Optional[str]:
        """Return the name of the profile set as active.
        Returns:
            The active profile name or None, if no active profile is set.
        """

    @abstractmethod
    def activate_stack(self, stack_name: str) -> None:
        """Set the active stack for the active profile.
        Args:
            stack_name: name of the stack to activate
        Raises:
            KeyError: If the stack with the given name does not exist.
        """

    @property
    @abstractmethod
    def active_stack_name(self) -> Optional[str]:
        """Get the active stack name from the active profile.
        Returns:
            The active stack name or None if no active stack is set or if
            no active profile is set.
        """