import os
import pickle
import sklearn as sk
import torch
import torch.nn as nn

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.logger import get_logger
from coalescenceml.io import fileio

from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "data.pt"


@register_producer_class
class PyTorchProducer(BaseProducer):
    """Read/Write Pytorch files."""

    ARTIFACT_TYPES = (
        ModelArtifact
    )
    TYPES = (nn.Module,)

    def handle_input(self, data_type: Type[Any]) -> nn.Module:
        """Reads pytorch model from sav file."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "rb") as fp:
          contents = torch.load(fp)
        #contents = torch.load(filepath)
        #model = TheModelClass(*args, **kwargs)
        #contents = model.load_state_dict(torch.load(filepath))
        #torch.load(open(filepath, 'rb'))
        return contents

    def handle_return(self, model: nn.Module) -> None:
        """Writes a pytorch model to artifact store as sav"""
        super().handle_return(model)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        type(model).__name__
        #torch.save(model.state_dict(), filepath)
        with fileio.open(filepath, "wb") as fp:
          torch.save(model, fp)

        #pickle.dump(model, open(filepath, 'wb'))
