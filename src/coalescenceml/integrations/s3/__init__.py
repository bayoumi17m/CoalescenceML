from coalescenceml.integrations.constants import S3
from coalescenceml.integrations.integration import Integration


class S3Integration(Integration):
    """Integration for AWS."""

    NAME = S3
    REQUIREMENTS = ["s3fs==2022.3.0"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.s3 import artifact_store


S3Integration.check_installation()
