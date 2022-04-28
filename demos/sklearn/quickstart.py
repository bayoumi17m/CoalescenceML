import numpy as np

from sklearn.base import BaseEstimator

from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.constants import SKLEARN
from coalescenceml.integrations.sklearn.step import (
    SKLearnTrainConfig,
    SKLearnTrainStep,
)

@step
def importer() -> Output(
    X_train=np.ndarray, y_train=np.ndarray, X_test=np.ndarray, y_test=np.ndarray
):
    scaling_factor = 0.2
    X = np.linspace(-5, 5, 101).reshape((-1, 1))
    Y = np.sin(X) + scaling_factor * np.random.randn(*X.shape)
    X = np.hstack([X, np.power(X, 2), np.power(X, 3)])
    X_train, y_train = X[:81], Y[:81]
    X_test, y_test = X[81:], Y[81:]

    return X_train, y_train, X_test, y_test


def partial_derivative(X: np.ndarray, Y: np.ndarray, model: np.ndarray) -> np.ndarray:
    Y_prime = X @ model
    n = X.shape[0]
    # print(Y)
    # print(Y_prime)
    # print(X.T)
    dl_dm = (-2/n) * (X.T @ (Y - Y_prime))
    dl_dm = dl_dm.reshape((len(dl_dm), -1)) + model

    return dl_dm


@step
def trainer(X_train: np.ndarray, y_train: np.ndarray) -> Output(model=np.ndarray):
    # model = np.linalg.inv(X_train.T @ X_train) @ X_train.T @ y_train
    feature_dim = X_train.shape[1]
    model = 0.01 * np.random.randn(feature_dim, 1)
    diff = float("inf")
    max_diff = 1e-8
    epochs = 500
    lr = 1e-5

    for epoch in range(epochs):
    # while diff > max_diff:
        # Compute Gradient
        derivative = partial_derivative(X_train, y_train, model)
        # print(derivative)
        # Update rule
        # diff = np.linalg.norm((-lr*derivative).flatten(), ord=2)
        model = model - lr*derivative

    return model


@step
def evaluator(
    model: BaseEstimator,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Output(mse=float):
    # mse = np.mean(np.power(X_test@model - y_test, 2))
    preds = model.predict(X_test)
    mse = np.mean(np.power(preds - y_test, 2))
    print(f"Test MSE: {mse:.2f}")
    return mse


@pipeline(required_integrations=[SKLEARN])
def sample_pipeline(importer, trainer, evaluator):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(x=X_train, y=y_train)
    mse = evaluator(model, X_test, y_test)


if __name__ == '__main__':
    sklearn_train_config = SKLearnTrainConfig(
        model_name = "linear_reg",
        hyperparams = {
            "fit_intercept": False,
        }
    )

    pipe = sample_pipeline(
            importer=importer(),
            trainer=SKLearnTrainStep(sklearn_train_config),
            evaluator=evaluator()
        )
    pipe.run()
