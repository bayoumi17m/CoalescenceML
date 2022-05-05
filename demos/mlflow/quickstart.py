import numpy as np

from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.mlflow.step.localdeployer import LocalDeployer
from coalescenceml.integrations.mlflow.step.kubedeployer import KubernetesDeployer, KubernetesDeployerConfig
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import DeployerConfig

from sklearn.base import BaseEstimator
from sklearn.linear_model import LinearRegression
import mlflow


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
    dl_dm = (-2 / n) * (X.T @ (Y - Y_prime))
    dl_dm = dl_dm.reshape((len(dl_dm), -1)) + model

    return dl_dm


@step
def trainer(X_train: np.ndarray, y_train: np.ndarray) -> Output(model=BaseEstimator):
    model = LinearRegression()
    model.fit(X_train, y_train)
    return model


@pipeline
def sample_pipeline(importer, trainer, deployer):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    # mlflow.set_tracking_uri("/Users/rafaelchaves/Library/Application Support/CoalescenceML/mlflow_runs")
    # with mlflow.start_run():
    #     mlflow.sklearn.log_model(model, "model")
    deploy_info = deployer()
    # print(deploy_info)


if __name__ == '__main__':
    # mlflow_deploy_config = KubernetesDeployerConfig(
    #     model_uri="s3://coml-mlflow-models/sklearn-regression-model",
    #     registry_path="us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model",
    #     deploy=True
    # )
    mlflow_deploy_config = DeployerConfig(
        model_uri="s3://coml-mlflow-models/sklearn-regression-model",
        registry_path="sklearn-model",
        deploy=True
    )
    pipe = sample_pipeline(
        importer=importer(),
        trainer=trainer(),
        deployer=LocalDeployer(mlflow_deploy_config)
    )
    pipe.run()
