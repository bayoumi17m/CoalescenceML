"""
placeholder init message
"""
from coalescenceml.integrations.constants import PYTORCH_L
from coalescenceml.integrations.integration import Integration

class PyTorchLightningIntegration(Integration):
    """Integration class for PyTorchLightning"""

    NAME = PYTORCH_L

    REQUIREMENTS = ["pytorch_lightning==1.6.2"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.pytorch_lightning import producers

PyTorchLightningIntegration.check_installation()