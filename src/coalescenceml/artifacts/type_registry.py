"""
The below code is inspired by: https://pypi.org/project/type-registry/
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple, Type

from coalescenceml.logger import get_logger

if TYPE_CHECKING:
    from coalescenceml.artifacts.base_artifact import BaseArtifact

logger = get_logger(__name__)


class TypeRegistry(object):
    """Registry to map classes into artifact types"""

    def __init__(self, artifact_types: Optional[Dict[Type[Any], Iterable[Type[BaseArtifact]]]]=None) -> None:
        """Initialize with some artifact types"""
        if artifact_types:
            self._artifact_types: Dict[Type[Any], Iterable[Type[BaseArtifact]]] = artifact_types
        else:
            self._artifact_types: Dict[Type[Any], Iterable[Type[BaseArtifact]]] = {}


    def get_artifact_type(self, key: Type[Any]) -> Tuple[Type[BaseArtifact], ...]:
        """
        """
        if key in self._artifact_types:
            return self._artifact_types[key]
        else:
            # Check for superclasses; But what happens if they subclass 2 things??
            artifact_types_for_superclasses = {
                artifact_type
                for registered_type, artifact_type in self._artifact_types.items()
                if issubclass(key, registered_type)
            }

            if len(artifact_types_for_superclasses) == 1:
                return artifact_types_for_superclasses.pop()
            elif len(artifact_types_for_superclasses) > 1:
                raise RuntimeError("Too many")
            else:
                print(len(artifact_types_for_superclasses))
                raise RuntimeError("Too little")

    def register_artifact_type(
        self, key: Type[Any], type_: Iterable[Type[BaseArtifact]]
    ) -> None:
        """register_artifact_type _summary_

        _extended_summary_

        :param key: any type
        :param type_: list of artifact type that the given datatypes is
                associated with
        """
        self._artifact_types[key] = tuple(type_)

# Create a global registry
type_registry = TypeRegistry()
