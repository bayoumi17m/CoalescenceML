from coalescenceml.integrations.constants import PANDAS
from coalescenceml.integrations.integration import Integration

class PandasIntegration(Integration):
    """Integration class for Pandas"""

    NAME = PANDAS

    REQUIREMENTS = ["pandas==1.4.2"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.pandas import producers

PandasIntegration.check_installation()
