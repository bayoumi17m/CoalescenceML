from coalescenceml.integrations.constants import LIGHTGBM
from coalescenceml.integrations.integration import Integration


class LightGBMIntegration(Integration):
    """Lightgbm intrgration for CoalescenceML."""

    NAME = LIGHTGBM
    REQUIREMENTS = ["lightgbm==3.3.2"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.lightgbm import producers


LightGBMIntegration.check_installation()
