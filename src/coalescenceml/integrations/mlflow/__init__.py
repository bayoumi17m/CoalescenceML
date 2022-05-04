from coalescenceml.integrations.constants import MLFLOW
from coalescenceml.integrations.integration import Integration

class MLFlowIntegration(Integration):
    """ Integration for mlflow. """

    NAME = MLFLOW
    REQUIREMENTS = ["mlflow==1.25"]
    SYSTEM_REQUIREMENTS = { "docker-engine" : "docker", "kubectl" : "kubectl" }