from coalescenceml.integrations.constants import STATSMODELS
from coalescenceml.integrations.integration import Integration


class StatsmodelsIntegration(Integration):
    """Integration for statsmodels."""

    NAME = STATSMODELS
    REQUIREMENTS = ["statsmodels==0.13.2"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.statsmodels import producers


StatsmodelsIntegration.check_installation()
