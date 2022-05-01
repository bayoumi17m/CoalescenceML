"""
placeholder init message
"""
from coalescenceml.integrations.constants import TENSORFLOW
from coalescenceml.integrations.integration import Integration

class TFIntegration(Integration):
    """Integration class for TensorFlow"""

    NAME = TENSORFLOW

    REQUIREMENTS = ["tensorflow==2.8.0", "tensorlfow_io==0.24.0"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.tensorflow import producers

TFIntegration.check_installation()
