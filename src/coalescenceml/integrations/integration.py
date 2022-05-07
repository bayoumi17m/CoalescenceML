import shutil
from typing import Any, Dict, List, Tuple, Type, cast

import pkg_resources

from coalescenceml.integrations.registry import integration_registry
from coalescenceml.logger import get_logger


logger = get_logger(__name__)


class IntegrationMeta(type):
    """Metaclass responsible for registering different Integration
    subclasses"""

    def __new__(
        mcs, name: str, bases: Tuple[Type[Any], ...], dct: Dict[str, Any]
    ) -> "IntegrationMeta":
        """Hook into creation of an Integration class."""
        cls = cast(Type["Integration"], super().__new__(mcs, name, bases, dct))
        if name != "Integration":
            integration_registry.register_integration(cls.NAME, cls)
        return cls


class Integration(metaclass=IntegrationMeta):
    """Base class for integration in CoalescenceML"""

    NAME = "base_integration"

    REQUIREMENTS: List[str] = []

    # tool name : command to check
    SYSTEM_REQUIREMENTS: Dict[str, str] = {}

    @classmethod
    def check_installation(cls) -> bool:
        """Method to check whether the required packages are installed"""
        try:
            for requirement, command in cls.SYSTEM_REQUIREMENTS.items():
                result = shutil.which(command)

                if result is None:
                    logger.debug(
                        "Unable to find the required packages for %s on your "
                        "system. Please install the packages on your system "
                        "and try again.",
                        requirement,
                    )
                    return False

            for r in cls.REQUIREMENTS:
                pkg_resources.get_distribution(r)
            logger.debug(
                f"Integration {cls.NAME} is installed correctly with "
                f"requirements {cls.REQUIREMENTS}."
            )
            return True
        except pkg_resources.DistributionNotFound as e:
            logger.debug(
                f"Unable to find required package '{e.req}' for "
                f"integration {cls.NAME}."
            )
            return False
        except pkg_resources.VersionConflict as e:
            logger.debug(
                f"VersionConflict error when loading installation {cls.NAME}: "
                f"{str(e)}"
            )
            return False

    @staticmethod
    def activate() -> None:
        """Abstract method to activate the integration"""
