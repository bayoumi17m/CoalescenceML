from typing import Any, Dict

from ml_metadata.proto import metadata_store_pb2
from tfx.types.artifact import Artifact, Property, PropertyType

from coalescenceml.artifacts.constants import (
    DATATYPE_PROPERTY_KEY,
    PRODUCER_PROPERTY_KEY,
)


DATATYPE_PROPERTY = Property(type=PropertyType.STRING)
PRODUCER_PROPERTY = Property(type=PropertyType.STRING)


class BaseArtifact(Artifact):
    """ """

    TYPE_NAME: str = "BaseArtifact"
    PROPERTIES: Dict[str, Property] = {
        DATATYPE_PROPERTY_KEY: DATATYPE_PROPERTY,
        PRODUCER_PROPERTY_KEY: PRODUCER_PROPERTY,
    }
    MLMD_TYPE: Any = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """"""
        self.validate_and_set_type()
        super(BaseArtifact, self).__init__(*args, **kwargs)

    @classmethod
    def validate_and_set_type(cls) -> None:
        """Validate artifact and set type"""
        type_name = cls.TYPE_NAME
        if not isinstance(type_name, str):
            raise ValueError(
                f"The subclass {cls} must overrise TYPE_NAME attribute with a string type name (got {type_name} instead)"
            )

        # Create ml metadata artifact type
        mlmd_artifact_type = metadata_store_pb2.ArtifactType()
        mlmd_artifact_type.name = type_name  # store the name

        # Perform validation on properties
        if cls.PROPERTIES:
            if not isinstance(cls.PROPERTIES, dict):
                raise ValueError(f"The subclass {cls}.PROPERTIES is not a dict")

            for key, value in cls.PROPERTIES.items():
                if not (isinstance(key, str) and isinstance(value, Property)):
                    raise ValueError(
                        f"The subclass {cls}.PROPERTIES dictionary must have keys of type string and values of type tfx.types.artifact.Property"
                    )

            # Finally populate ML metadata properties
            for key, value in cls.PROPERTIES.items():
                mlmd_artifact_type.properties[key] = value.mlmd_type()
        else:
            raise ValueError("Empty properties dictionary!")

        cls.MLMD_ARTIFACT_TYPE = mlmd_artifact_type
