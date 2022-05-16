import re
from typing import ClassVar

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import StackComponent
from coalescenceml.utils import docker_utils


class BaseContainerRegistry(StackComponent):
    """Base class for all CoalescenceML container registries.
    Attributes:
        uri: The URI of the container registry.
    """

    uri: str

    # Class Configuration
    TYPE: ClassVar[
        StackComponentFlavor
    ] = StackComponentFlavor.CONTAINER_REGISTRY
    FLAVOR: ClassVar[str] = "default"

    @property
    def is_local(self) -> bool:
        return re.fullmatch(r"localhost:[0-9]{4,5}", self.uri)

    def push_image(self, image_name: str) -> None:
        """Push a docker image.

        Args:
            image_name: name of the docker image that will be pushed.
        
        Raises:
            ValueError: If the image name is not associated with this registry
        """
        if not image_name.startswith(self.uri):
            raise ValueError(
                ""
            )
        
        docker_utils.push_docker_image(image_name)
