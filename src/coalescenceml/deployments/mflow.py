import base
import mlflow

class MLFlowModelObject(base.ModelObject):

  def __init__(self):
    pass

  def load_context(self, model_uri):
    self.model = mlflow.pyfunc.load_model(model_uri)

  def predict(self, data):
    # TODO: log input
    predictions = self.model.predict(data)
    # TODO: log predictions
    return predictions

  def get_features(self, raw_features):
    pass

  def explain(self):
    pass