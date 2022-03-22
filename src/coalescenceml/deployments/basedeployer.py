from abc import ABC, abstractmethod
from modelobject import ModelObject
from typing import Optional

# inherit base profile step?


class BaseDeploymentStep(ABC):
    """Base class for a deployment step in a CoML pipeline. """

    @abstractmethod
    def entry_point(self, model_uri: str, registry_path: str, deploy: bool) -> Optional[str]:
        """Deploys model specified by uri, if deploy is True.

        The model is built into an image at the given path, pushed
        to the registry, and deployed to the Kubernetes cluster specified
        in the kubeconfig file.

        Returns:
          A url for the deployment, if successful, otherwise None.
        """
        pass
