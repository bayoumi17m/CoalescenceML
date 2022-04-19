from __future__ import annotations

from inspect import isclass
from typing import Any, Dict, Type, TypeVar

# if TYPE_CHECKING:
from coalescenceml.artifacts.base_artifact import BaseArtifact
from coalescenceml.artifacts.type_registry import type_registry

# Exception
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.exceptions import ProducerInterfaceError


logger = get_logger(__name__)


class ProducerRegistry(object):
    """Matches a type to a default producer."""

    def __init__(self) -> None:
        self.producer_types: Dict[Type[Any], Type[BaseProducer]] = {}

    def register_producer(
        self, object_type: Type[Any], producer: BaseProducer
    ) -> None:
        """Registers a new producer.

        Args:
            object_type: Indicates python type of object.
            producer: A BaseProducer subclass for the python type.
        """
        if object_type in self.producer_types:
            logger.debug(
                f"Found existing producer for {object_type}: {self.producer_types[object_type]}. Skipping registration of {producer}."
            )
        else:
            self.producer_types[object_type] = producer
            logger.debug(f"Registered producer {producer} for {object_type}.")

    def register_overwrite_producer(
        self, object_type: Type[Any], producer: BaseProducer
    ) -> None:
        """Registers a new producer and overwrites any prior setting.

        Args:
            object_type: Indicates python type of object.
            producer: A BaseProducer subclass for the python type.
        """
        self.producer_types[object_type] = producer
        logger.debug(f"Registered producer {producer} for {object_type}.")

    def get_producers(self) -> Dict[Type[Any], Type[BaseProducer]]:
        """Get all registered producers."""
        return self.producer_types

    def is_registered(self, object_type: Type[Any]) -> bool:
        """Returns if a producer is registered for the given type.

        Args:
            object_type: Indicates python type of object.

        Returns:
            bool: Whether a producer class is registered for the python type
        """
        return any(issubclass(object_type, p) for p in self.producer_types)

    def __getitem__(self, object_type: Type[Any]) -> BaseProducer:
        """Retrieve a single producer based on the python type.

        Args:
            object_type: Indicates python type of object.

        Returns:
            `BaseProducer` subclass that was registered for this type.

        Raises:
            ValueError: If the type (or any of its superclasses) is not registered or the type has more than one superclass with different producers.
        """
        if object_type in self.producer_types:
            return self.producer_types[object_type]
        else:
            # if type is not registered then check for superclasses
            producers_compatible_superclass = {
                producer
                for registered_type, producer in self.producer_types.items()
                if issubclass(object_type, registered_type)
            }
            if len(producers_compatible_superclass) == 1:
                return producers_compatible_superclass.pop()
            elif len(producers_compatible_superclass) > 1:
                raise ValueError(
                    f"Type {object_type} is subclassing more than one type, this it has multiple producers within this registery: {producers_compatible_superclass}."
                )

        raise KeyError(
            f"No producer is registered for object type {object_type}. You can register a producer for specific types by subclassing `BaseProducer` and setting its `ASSOCIATED_TYPES` attribute."
        )


C = TypeVar("C", bound=BaseProducer)
producer_registry = ProducerRegistry()


def register_producer_class(cls: Type[C]) -> Type[C]:
    """Registers the producer class and returns it unmodified."""
    if not cls.TYPES:
        raise ProducerInterfaceError(
            f"Invalid producer. When defining producer, make sure to specify at least 1 type in the TYPES class variable."
        )

    for artifact_type in cls.ARTIFACT_TYPES:
        if not (
            isclass(artifact_type) and issubclass(artifact_type, BaseArtifact)
        ):
            raise ProducerInterfaceError(
                f"Associated artifact type {artifact_type} for producer is not a class or is not a subclass of BaseArtifact."
            )

    artifact_types = cls.ARTIFACT_TYPES or (BaseArtifact,)
    for t in cls.TYPES:
        if not isclass(t):
            raise ProducerInterfaceError(
                f"Associated type {t} for producer is not a class."
            )

        producer_registry.register_producer(
            t,
            cls,
        )
        type_registry.register_artifact_type(
            t,
            artifact_types,
        )

    return cls
