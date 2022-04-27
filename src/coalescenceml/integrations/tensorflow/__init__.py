"""
placeholder init message
"""
from coalescenceml.integrations.constants import TENSORFLOW
from coalescenceml.integrations.integration import Integration

class TFIntegration(Integration):
    """Integration class for TensorFlow"""

    NAME = TENSORFLOW

    # might need to add tensorflow i/o as requirement 
    REQUIREMENTS: List[str] = ["tensorflow==2.8.0"]

    SYSTEM_REQUIREMENTS: Dict[str, str] = {}

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.tensorflow.producers import TFProducer

TFIntegration.check_installation()
