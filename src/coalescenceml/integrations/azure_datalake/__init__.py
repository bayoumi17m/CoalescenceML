from coalescenceml.integrations.constants import AZURE
from coalescenceml.integrations.integration import Integration


class AzureIntegration(Integration):
    """Integration for AZURE."""

    NAME = AZURE
    REQUIREMENTS = ["adlfs==2022.4.0"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.azure_datalake import artifact_store


AzureIntegration.check_installation()