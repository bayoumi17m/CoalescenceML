"""
placeholder init message
"""
class SKLearnIntegration(Integration):
    """Integration class for SKLearn"""

    NAME = "sklearn_integration"

    REQUIREMENTS: List[str] = ["sklearn == 1.0.2"]

    SYSTEM_REQUIREMENTS: Dict[str, str] = {}

def activate() -> None:
    from coalescenceml.integrations.sklearn.producers import SKLearnProducer