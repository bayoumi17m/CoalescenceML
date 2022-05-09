from coalescenceml.integrations.constants import WANDB
from coalescenceml.integrations.integration import Integration


class WandbIntegration(Integration):
    """Integration for Wandb."""

    NAME = WANDB
    REQUIREMENTS = ["wandb==0.12.15"]

    @classmethod
    def activate(cls) -> None:
        from coalescenceml.integrations.wandb import experiment_tracker


WandbIntegration.check_installation()
