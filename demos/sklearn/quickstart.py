import numpy as np

from sklearn.base import BaseEstimator
from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step
from coalescenceml.integrations.constants import SKLEARN
from coalescenceml.integrations.sklearn.step import (
    SKLearnTrainConfig,
    SKLearnTrainStep,
)

@step
def importer() -> Output(
    X_train=np.ndarray, y_train=np.ndarray, X_test=np.ndarray, y_test=np.ndarray
):
    X, Y = make_regression(
        n_samples=500, n_features=3, n_informative=3, noise=0.2
    )
    X_train, X_test, y_train, y_test = train_test_split(
            X, Y, test_size=0.2, random_state=621
    )

    return X_train, y_train, X_test, y_test


class LinearRegressionConfig(BaseStepConfig):
    fit_intercept: bool = False


@step
def trainer(
    config: LinearRegressionConfig,
    X_train: np.ndarray,
    y_train: np.ndarray
) -> Output(model=LinearRegression):
    """"""
    model = LinearRegression(**config.dict())
    model.fit(X_train, y_train)

    return model


@step
def evaluator(
    model: BaseEstimator,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Output(mse=float):
    # mse = np.mean(np.power(X_test@model - y_test, 2))
    preds = model.predict(X_test)
    mse = mean_squared_error(y_test, preds)
    print(f"Test MSE: {mse:.2f}")
    return mse


@pipeline(required_integrations=[SKLEARN])
def sample_pipeline(importer, trainer, evaluator):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    mse = evaluator(model, X_test, y_test)


if __name__ == '__main__':
    sklearn_train_config = SKLearnTrainConfig(
        model_name = "linear_reg",
        hyperparams = {
            "fit_intercept": False,
        }
    )

    # Use SKLearnTrainStep
    pipe_1 = sample_pipeline(
        importer=importer(),
        trainer=SKLearnTrainStep(sklearn_train_config),
        evaluator=evaluator()
    )
    pipe_1.run()


    # Use user defined sklearn step
    pipe_2 = sample_pipeline(
        importer=importer(),
        trainer=trainer(),
        evaluator=evaluator(),
    )
    pipe_2.run()
