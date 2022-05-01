import os
import pytorch_lightning as pl
from typing import Any, Type

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "data.ckpt"


@register_producer_class
class PyTorchLightningProducer(BaseProducer):
    """Read/Write PyTorch Lightning files."""

    ARTIFACT_TYPES = (ModelArtifact,)
    TYPES = (pl.Trainer)

    def handle_input(self, data_type: Type[Any]) -> pl.Trainer:
        """Reads PyTorch Lightning trainer from ckpt file."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "rb") as fp:
        # resume_from_checkpoint will be deprecated in v2.0
            trainer = Trainer.resume_from_checkpoint(checkpoint_path=fp)
        return trainer

    def handle_return(self, trainer: pl.Trainer) -> None:
        """Writes a PyTorch Lightning trainer to artifact store as ckpt"""
        super().handle_return(trainer)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "wb") as fp:
            trainer.save_checkpoint(filepath)