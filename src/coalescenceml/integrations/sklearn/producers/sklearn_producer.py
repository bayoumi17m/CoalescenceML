import os
import pickle
import sklearn as sk
from sklearn.base import BaseEstimator, ClassifierMixin, RegressorMixin
from typing import Any, Type

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "data.sav"


@register_producer_class
class SKLearnProducer(BaseProducer):
    """Read/Write SKLearn files."""

    ARTIFACT_TYPES = (
        ModelArtifact,
    )
    TYPES = (BaseEstimator,)

    def handle_input(self, data_type: Type[Any]) -> BaseEstimator:
        """Reads sklearn model from sav file."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "rb") as fp:
            contents = pickle.load(fp)
        return contents

    def handle_return(self, model: BaseEstimator) -> None:
        """Writes a sklearn model to artifact store as sav"""
        super().handle_return(model)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "wb") as fp:
            pickle.dump(model, fp)
