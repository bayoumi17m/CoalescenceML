"""
placeholder init message
"""
from coalescenceml.integrations.constants import EVIDENTLY
from coalescenceml.integrations.integration import Integration

class TFIntegration(Integration):
    """Integration class for Evidently"""

    NAME = EVIDENTLY

    REQUIREMENTS = ["evidently==0.1.48.dev0"]

    @classmethod
    def activate(cls) -> None:
        pass

TFIntegration.check_installation()
