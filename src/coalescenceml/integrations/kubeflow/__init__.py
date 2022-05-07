"""
placeholder init message
"""
# Imports here when we have them
from coalescenceml.integrations.constants import KUBEFLOW
from coalescenceml.integrations.integration import Integration


class KubeflowIntegration(Integration):
    """Integration class for Kubeflow"""

    NAME = KUBEFLOW

    REQUIREMENTS = ["kfp==1.8.12"]

    @classmethod
    def activate(cls) -> None:
        """
        Activate Kubeflow
        """
        pass


if not KubeflowIntegration().check_installation():
    raise ImportError(
        "Unable to find the required packages for Kubeflow on your system. "
        "Please install the packages on your system and try again."
    )
