"""
placeholder init message
"""
from coalescenceml.integrations.constants import SKLEARN
from coalescenceml.integrations.integration import Integration

class SKLearnIntegration(Integration):
    """Integration class for SKLearn"""

    NAME = SKLEARN

    REQUIREMENTS: List[str] = ["sklearn==1.0.2"]

    SYSTEM_REQUIREMENTS: Dict[str, str] = {}

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.sklearn.producers import SKLearnProducer

SKLearnIntegration.check_installation()