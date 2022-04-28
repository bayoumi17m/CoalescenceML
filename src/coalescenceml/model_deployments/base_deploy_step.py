from coalescenceml.step.base_step import BaseStep
from coalescenceml.step import BaseStepConfig
from abc import abstractmethod
from typing import Any, Dict


class BaseDeploymentStep(BaseStep):
    """Base class for a deployment step in a CoalescenceML pipeline. """

    @abstractmethod
    def entrypoint(self, config: BaseStepConfig) -> Dict[str, Any]:
        """Deploys model specified in config.

        The model is built into an image at the given path, pushed
        to the registry, and deployed to the Kubernetes cluster specified
        in the kubeconfig file.

        Returns:
          A dictionary containing deployment info.
        """
        pass
