"""A demo for logging and deploying an MLFlow model locally. """
import os
from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.mlflow.step.localdeployer import LocalDeployer
from coalescenceml.integrations.mlflow.step.kubedeployer import KubernetesDeployer
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseDeployerConfig
from sklearn.base import BaseEstimator
from sklearn.linear_model import ElasticNet
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import mlflow

WINE_CSV = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"


@step
def importer() -> Output(
    X_train=pd.DataFrame, y_train=pd.DataFrame, X_test=pd.DataFrame, y_test=pd.DataFrame
):
    np.random.seed(40)
    data = pd.read_csv(WINE_CSV, sep=";")
    train, test = train_test_split(data)
    X_train = train.drop(["quality"], axis=1)
    X_test = test.drop(["quality"], axis=1)
    y_train = train[["quality"]]
    y_test = test[["quality"]]
    return X_train, y_train, X_test, y_test


@step
def trainer(X_train: pd.DataFrame, y_train: pd.DataFrame) -> Output(model=BaseEstimator):
    model = ElasticNet(alpha=0.5, l1_ratio=0.5, random_state=42)
    model.fit(X_train, y_train)
    return model


@step
def log_model(model: BaseEstimator) -> Output(model_uri=str):
    # tracking_uri = "/Users/rafaelchaves/Library/Application Support/CoalescenceML/mlflow_runs"
    # mlflow.set_tracking_uri(tracking_uri)
    # mlflow.sklearn.log_model(model, "model")
    # artifact_uri = mlflow.get_artifact_uri()
    # model_uri = os.path.join(artifact_uri, "model")
    model_uri = "s3://coml-mlflow-models/sklearn-regression-model"
    return model_uri


@pipeline
def sample_pipeline(importer, trainer, log_model, deployer):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    model_uri = log_model(model)
    deploy_info = deployer(model_uri)
    print(deploy_info)


if __name__ == '__main__':
    # /Users/rafaelchaves/Library/Application Support/CoalescenceML/mlflow_runs
    mlflow_deploy_config = BaseDeployerConfig(
        # image_name="sklearn_model_image"
    )
    pipe = sample_pipeline(
        importer=importer(),
        trainer=trainer(),
        log_model=log_model(),
        deployer=KubernetesDeployer(mlflow_deploy_config)
    )
    pipe.run()
