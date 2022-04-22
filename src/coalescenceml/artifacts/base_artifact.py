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
    """Base class for a CoalescenceML artifact.

    Every artifact type needs to subclass this class. When doing this, there
    are a few things to consider:

    - Each class needs to be given a unique TYPE_NAME.
    - The artifact can store different properties under the PROPERTIES
        attribute which will be tracked through a pipeline run.
    """

    TYPE_NAME: str = "BaseArtifact"
    PROPERTIES: Dict[str, Property] = {
        DATATYPE_PROPERTY_KEY: DATATYPE_PROPERTY,
        PRODUCER_PROPERTY_KEY: PRODUCER_PROPERTY,
    }
    MLMD_TYPE: Any = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create artifact class.

        Args:
            args: Pass through args for TFX artifact
            kwargs: pass through args for TFX artifact
        """
        self.validate_and_set_type()
        super(BaseArtifact, self).__init__(*args, **kwargs)

    @classmethod
    def validate_and_set_type(cls) -> None:
        """Validate artifact and set type.

        Raises:
            ValueError: if Artifact type is not a str that overrides
                the BaseArtifact name
        """
        type_name = cls.TYPE_NAME
        if not isinstance(type_name, str):
            raise ValueError(
                f"The subclass {cls} must override TYPE_NAME attribute with "
                f"a string type name (got {type_name} instead)"
            )

        # Create ml metadata artifact type
        mlmd_artifact_type = metadata_store_pb2.ArtifactType()
        mlmd_artifact_type.name = type_name  # store the name

        # Perform validation on properties
        if cls.PROPERTIES:
            if not isinstance(cls.PROPERTIES, dict):
                raise ValueError(
                    f"The subclass {cls}.PROPERTIES "
                    f"is not a dict"
                )

            for key, value in cls.PROPERTIES.items():
                if not (isinstance(key, str) and isinstance(value, Property)):
                    raise ValueError(
                        f"The subclass {cls}.PROPERTIES dictionary must have "
                        f"keys of type string and values of type "
                        f"tfx.types.artifact.Property"
                    )

            # Finally populate ML metadata properties
            for key, value in cls.PROPERTIES.items():
                mlmd_artifact_type.properties[key] = value.mlmd_type()
        else:
            raise ValueError("Empty properties dictionary!")

        cls.MLMD_ARTIFACT_TYPE = mlmd_artifact_type
