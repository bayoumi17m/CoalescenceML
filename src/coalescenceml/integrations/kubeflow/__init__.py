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
        from coalescenceml.integrations.kubeflow import producers


KubeflowIntegration.check_installation()
