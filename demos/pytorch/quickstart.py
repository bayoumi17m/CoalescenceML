import numpy as np
import torch
from sklearn.datasets import load_breast_cancer
from sklearn.preprocessing import StandardScaler
import torch.nn as nn

from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.constants import PYTORCH
from coalescenceml.integrations.pytorch import (
    PTClassifierConfig,
    PyTorchClassifierTrainStep,
)

@step
def importer() -> Output(
    X_train=np.ndarray, y_train=np.ndarray, X_test=np.ndarray, y_test=np.ndarray
):
    data = load_breast_cancer()
    x = data['data']
    y = data['target']

    sc = StandardScaler()
    x = sc.fit_transform(x)
    split = 300

    X_train = x[0:split, :]
    y_train = y[0:split]
    X_test = x[split:, :]
    y_test = y[split:]

    return X_train, y_train, X_test, y_test


@step
def pt_trainer(X_train: np.ndarray, y_train: np.ndarray) -> Output(model=nn.Module):
    config = PTClassifierConfig(layers=[32,64], num_classes=2, batch_size=17)

    classifierStep = PyTorchClassifierTrainStep()
    model = classifierStep.entrypoint(config, X_train, y_train)
    return model


@step
def pt_evaluator(
    model: nn.Module,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Output(test_acc=float):
    model.eval()
    acc = 0

    with torch.no_grad():
      for i in range(len(X_test)):
        y_pred = model(torch.Tensor(X_test[i]).unsqueeze(0))
        y_pred = round(y_pred.item())
        if (y_pred == y_test[i]):
          acc +=1
    
    return acc / len(X_test)


@pipeline(required_integrations=[PYTORCH])
def sample_pipeline(importer, trainer, evaluator):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    mse = evaluator(model, X_test, y_test)

if __name__ == '__main__':

    pipe_1 = sample_pipeline(
        importer=importer(),
        trainer=pt_trainer(),
        evaluator=pt_evaluator(),
    )
    pipe_1.run()