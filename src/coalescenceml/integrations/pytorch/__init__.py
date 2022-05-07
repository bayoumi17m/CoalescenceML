from coalescenceml.integrations.constants import PYTORCH
from coalescenceml.integrations.integration import Integration

class PTIntegration(Integration):
    """Integration class for PyTorch"""

    NAME = PYTORCH

    REQUIREMENTS = ["torch==1.11.0"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.pytorch import producers

PTIntegration.check_installation()
