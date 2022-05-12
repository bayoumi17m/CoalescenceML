from coalescenceml.integrations.constants import XGBOOST
from coalescenceml.integrations.integration import Integration


class XgboostIntegration(Integration):
    """XGBoost integration for Coalescence."""

    NAME = XGBOOST
    REQUIREMENTS = ["xgboost==1.6.0"]

    @classmethod
    def activate(cls) -> None:
        """Activate the integration."""
        from coalescenceml.integrations.xgboost import producers
