from coalescenceml.deployments import mlflowobject
from coalescenceml.exceptions import CoMLException
import numpy as np
import pandas as pd

columns = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar",
            "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density",
            "pH", "sulphates", "alcohol"]
data = [[6.2, 0.66, 0.48, 1.2, 0.029, 29, 75, 0.98, 3.33, 0.39, 12.8]]
df = pd.DataFrame(columns=columns, data=data)

def test_sklearn_regression_predict() -> None:
    model = mlflowobject.MLFlowModelObject()
    model.load_context("s3://coml-mlflow-models/sklearn-regression-model")
    assert np.allclose(model.predict(df), [5.72476731])

def test_sklearn_regression_no_ctx() -> None:
  model = mlflowobject.MLFlowModelObject()
  try:
    model.predict(df)
    assert False
  except CoMLException:
    pass

test_sklearn_regression_predict()
test_sklearn_regression_no_ctx()
