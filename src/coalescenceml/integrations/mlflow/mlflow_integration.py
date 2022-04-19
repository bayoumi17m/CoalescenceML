from coalescenceml.integrations.integration import Integration

class MLFlowIntegration(Integration):
    NAME = "mlflow_integration"
    REQUIREMENTS = ["mlflow==1.25"]
    SYSTEM_REQUIREMENTS = { "docker-engine" : "docker", "kubectl" : "kubectl" , "conda" : "conda"}