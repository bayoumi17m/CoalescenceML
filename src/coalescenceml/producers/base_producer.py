from typing import Any, ClassVar, Tuple, Type

from coalescenceml.artifacts.base_artifact import BaseArtifact


class BaseProducer(object):
    """BaseProducer to produce artifact data."""

    ARTIFACT_TYPES: ClassVar[Tuple[Type[BaseArtifact], ...]] = ()
    TYPES: ClassVar[Tuple[Type[Any], ...]] = ()

    def __init__(self, artifact: BaseArtifact):        
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

    def handle_input(self, data: Type[Any]) -> Any:
        """
        """
        if not self.can_handle_type(data):
            raise TypeError(f"Unable to handle type {data}.")

    def handle_return(self, data: Any) -> None:
        """
        """
        data_type = type(data)
        if not self.can_handle_type(data_type):
            raise TypeError(f"Unable to handle type {data}.")
