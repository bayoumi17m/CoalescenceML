from abc import abstractmethod
from inspect import isclass
from typing import Any, ClassVar, Dict, Tuple, Type, cast

from coalescenceml.artifacts.base_artifact import BaseArtifact
# from coalescenceml.artifacts.type_registry import type_registry
# from coalescenceml.producers.producer_registry import producer_registry


class BaseProducer(object):
    """BaseProducer to produce artifact data."""

    ARTIFACT_TYPES: ClassVar[Tuple[Type[BaseArtifact], ...]] = ()
    TYPES: ClassVar[Tuple[Type[Any], ...]] = ()

    def __init__(self, artifact: BaseArtifact):
        # if not self.TYPES:
        #     raise ValueError(
        #         f"Invalid producer. When defining producer, make sure to specify at least 1 type in the TYPES class variable."
        #     )

        # for artifact_type in self.ARTIFACT_TYPES:
        #     if not (
        #         isclass(artifact_type)
        #         and issubclass(artifact_type, BaseArtifact)
        #     ):
        #         raise ValueError(
        #             f"Associated artifact type {artifact_type} for producer is not a class or is not a subclass of BaseArtifact."
        #         )
        
        # artifact_types = self.ARTIFACT_TYPES or (BaseArtifact,)
        # for t in self.TYPES:
        #     if not isclass(t):
        #         raise ValueError(
        #             f"Associated type {t} for producer is not a class."
        #         )

        #     producer_registry.register_producer(
        #         t, self,
        #     )
        #     type_registry.register_artifact_type(
        #         t, artifact_types,
        #     )
        
        self.artifact = artifact


    def can_handle_type(self, data_type: Type[Any]) -> bool:
        """can_handle_type determines whether the producer can r/w a certain type.

        Args:
            data_type: the type to check for

        Returns:
            can_handle: Whether the type can be r/w
        """
        return any(
            issubclass(data_type, t)
            for t in self.TYPES
        )

    @abstractmethod
    def handle_input(self, data: Type[Any]) -> Any:
        """
        """
        if not self.can_handle_type(data):
            raise ValueError(f"Unable to handle type {data}.")

    @abstractmethod
    def handle_return(self, data: Any) -> None:
        """
        """
        data_type = type(data)
        if not self.can_handle_type(data_type):
            raise ValueError(f"Unable to handle type {data}.")
