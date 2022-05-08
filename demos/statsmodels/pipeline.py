import numpy as np
from statsmodels.base.wrapper import ResultsWrapper
import statsmodels.api as sm

from coalescenceml.step import step, Output
from coalescenceml.pipeline import pipeline
from coalescenceml.integrations.constants import STATSMODELS


@step
def importer() -> Output(
    X_train=np.ndarray, y_train=np.ndarray,
    X_test=np.ndarray, y_test=np.ndarray,
):
    nsample = 500
    x = np.linspace(0, 10, nsample)
    X = np.column_stack((x, x ** 2))
    beta = np.array([1, 0.1, 10])
    e = np.random.normal(size=nsample)

    X = sm.add_constant(X)
    y = np.dot(X, beta) + e

    train_indices = np.random.randint(nsample, size=int(0.8*nsample))
    train_mask = np.zeros((nsample,)).astype(bool)
    train_mask[train_indices] = True

    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[~train_mask], y[~train_mask]

    return X_train, y_train, X_test, y_test


@step
def trainer(X: np.ndarray, y: np.ndarray) -> Output(model=ResultsWrapper):
    model = sm.OLS(y, X).fit()

    return model


@step
def evaluator(X: np.ndarray, y: np.ndarray, model: ResultsWrapper)->Output(
    mse=float
):
    ypred = model.predict(X)
    mse = np.mean(
        np.power(y - ypred, 2)
    )
    print(f"The test mse is: {mse:.2f}")
    return mse


@pipeline(required_integrations=[STATSMODELS])
def sample_pipeline(
    importer,
    trainer,
    evaluator
):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    mse = evaluator(X_test, y_test, model)


if __name__ == '__main__':
    pipe = sample_pipeline(
            importer=importer(),
            trainer=trainer(),
            evaluator=evaluator(),
    )

    pipe.run()

