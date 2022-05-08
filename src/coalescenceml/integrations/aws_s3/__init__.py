from coalescenceml.integrations.constants import AWS
from coalescenceml.integrations.integration import Integration


class AWSIntegration(Integration):
    """Integration for AWS."""

    NAME = AWS
    REQUIREMENTS = ["s3fs"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.aws_s3 import artifact_store


AWSIntegration.check_installation()