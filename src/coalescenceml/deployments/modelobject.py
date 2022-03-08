from abc import ABC, abstractmethod


class ModelObject(ABC):
    """Base class for CoalescenceML model objects."""

    @abstractmethod
    def load_context(self, model_uri):
        """Load model from specified uri."""
        pass

    @abstractmethod
    def predict(self, data):
        """
        Output model predictions on given data.

        Args: 
          data: A pandas DataFrame or a numpy array.
        """
        pass

    @abstractmethod
    def get_features(self, raw_features):
        """
        Apply transformations to raw features.

        Args:
          raw_features: A string, dictionary, pandas DataFrame, or numpy array.
        Returns:
          A pandas DataFrame or numpy array.
        """
        pass

    @abstractmethod
    def explain(self):
        pass
