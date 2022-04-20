"""
placeholder init message
"""
from coalescenceml.integrations.constants import SKLEARN
from coalescenceml.integrations.integration import Integration

class SKLearnIntegration(Integration):
    """Integration class for SKLearn"""

    NAME = SKLEARN

    REQUIREMENTS = ["scikit-learn==1.0.2"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.sklearn import producers

SKLearnIntegration.check_installation()
