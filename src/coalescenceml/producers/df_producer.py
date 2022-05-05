from __future__ import annotations

from typing import Any, Type
import pandas as pd
import os

from coalescenceml.artifacts import DataArtifact
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class

DATA_CSV_FILENAME = "data.csv"
DATA_VAR = "data_var"


@register_producer_class
class DataFrameProducer(BaseProducer):
    """Producer to read data to and from Pandas dataframes."""

    ARTIFACT_TYPES = (DataArtifact,)
    TYPES = (pd.DataFrame,)

    def handle_input(self, data_type: Type[Any]) -> pd.DataFrame:
        """Reads pandas dataframe from csv file.

        Args:
            data_type: type of input to be processed.

        Returns:
            Pandas DataFrame with data
        """
        super().handle_input(data_type)
        return pd.read_csv(os.path.join(self.artifact.uri, DATA_CSV_FILENAME))

    def handle_return(self, df: pd.DataFrame) -> None:
        """Writes a pandas DataFrame to artifact store as csv.

        Args:
            df: Pandas DataFrame to write.
        """
        super().handle_return(df)
        df.to_csv(os.path.join(self.artifact.uri, DATA_CSV_FILENAME), index=False)
