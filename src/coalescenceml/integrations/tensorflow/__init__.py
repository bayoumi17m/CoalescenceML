"""
placeholder init message
"""
class TFIntegration(Integration):
    """Integration class for TensorFlow"""

    NAME = "tensorflow_integration"

    # might need to add tensorflow i/o as requirement 
    REQUIREMENTS: List[str] = ["tensorflow == 2.8.0"]

    SYSTEM_REQUIREMENTS: Dict[str, str] = {}

def activate() -> None:
    from coalescenceml.integrations.tensorflow.producers import TFProducer