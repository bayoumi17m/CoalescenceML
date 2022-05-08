from typing import Any, Type
import os
import pickle

import statsmodels as sm
from statsmodels.base.wrapper import ResultsWrapper

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.producers import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class

logger = get_logger(__name__)
DEFAULT_FILENAME = "model.pkl" # TODO: Can the users specify a file name?


@register_producer_class
class StatsmodelsProducer(BaseProducer):
    """Producer to handle statsmodels model classes"""
    
    ARTIFACT_TYPES = (ModelArtifact,)
    TYPES = (ResultsWrapper,)

    def handle_input(self, data_type: Type[Any]) -> ResultsWrapper:
        """Process artifact of data_type into a Statsmodel object.

        Args:
            data_type: data type to check and process

        Returns:
            ResultsWrapper from statsmodels
        """
        super().handle_input(data_type)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "rb") as fp:
            contents = pickle.load(fp) # TODO: Can we make this more secure somehow?

        return contents

    def handle_return(self, model: ResultsWrapper) -> None:
        """Write statsmodels model to artifact store as pickle.

        Args:
            model: Statsmodels model to save.
        """
        super().handle_return(model)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        with fileio.open(filepath, "wb") as fp:
            pickle.dump(model, fp)
