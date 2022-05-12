from coalescenceml.deployments import modelobject
from coalescenceml.exceptions import CoMLException
import mlflow


class MLFlowModelObject(modelobject.ModelObject):

    def __init__(self):
        self.model = None

    def load_context(self, model_uri):
        self.model = mlflow.pyfunc.load_model(model_uri)

    def predict(self, data):
        if self.model is None:
            raise CoMLException(
                "Model not loaded. Please load a model with load_context.")
        # TODO: log input
        predictions = self.model.predict(data)
        # TODO: log predictions
        return predictions

    def get_features(self, raw_features):
        pass

    def explain(self):
        pass
