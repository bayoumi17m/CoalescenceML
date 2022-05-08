from coalescenceml.integrations.constants import MLFLOW
from coalescenceml.integrations.integration import Integration


class MLFlowIntegration(Integration):
    """MLFlow integration for CoalescenceML."""

    NAME = MLFLOW
    REQUIREMENTS = [
        "mlflow==1.25.1",
        "mlserver==1.0.1",
        "mlserver-mlflow==1.0.1",
    ]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.mlflow import experiment_tracker


MLFlowIntegration.check_installation()
