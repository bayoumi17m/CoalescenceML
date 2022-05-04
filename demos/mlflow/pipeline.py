from typing import Optional

import mlflow
import numpy as np
from sklearn.datasets import load_breast_cancer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from coalescenceml.integrations.constants import MLFLOW, SKLEARN
from coalescenceml.integrations.mlflow.mlflow_step_decorator import enable_mlflow
from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step


@step
def import_data() -> Output(
    x_train=np.ndarray, y_train=np.ndarray,
    x_test=np.ndarray, y_test=np.ndarray,
):
    X, Y = load_breast_cancer(return_X_y=True)
    x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.3, random_state=0)
    return x_train, y_train, x_test, y_test


class RandomForestConfig(BaseStepConfig):
    n_estimators: int = 100
    criterion: str = "gini"
    max_depth: Optional[int] = None
    min_samples_split: int = 2


@enable_mlflow
@step
def trainer(
    config: RandomForestConfig,
    x_train: np.ndarray,
    y_train: np.ndarray,
) -> Output(model=RandomForestClassifier):
    mlflow.sklearn.autolog()

    model = RandomForestClassifier(**config.dict())
    model.fit(x_train, y_train)

    return model


@enable_mlflow
@step
def evaluator(
    model: RandomForestClassifier,
    x_test: np.ndarray,
    y_test: np.ndarray,
) -> Output(acc=float):
    preds = model.predict(x_test)
    test_acc = accuracy_score(y_test, preds)
    mlflow.log_metrics({"test_acc": test_acc})
    return test_acc


@pipeline(enable_cache=False, required_integrations=[MLFLOW, SKLEARN])
def mlflow_example_pipeline(
    importer,
    trainer,
    evaluator,
):
    x_train, y_train, x_test, y_test = importer()
    model = trainer(x_train, y_train)
    evaluator(model, x_test, y_test)


def get_tracking_uri() -> str:
    from coalescenceml.directory import Directory

    tracker = Directory().active_stack.experiment_tracker

    return tracker.get_tracking_uri()

if __name__ == '__main__':
    config_1 = RandomForestConfig(
    )

    config_2 = RandomForestConfig(
        n_estimators=200,
    )

    config_3 = RandomForestConfig(
        n_estimators=50,
    )

    config_4 = RandomForestConfig(
        max_depth=10,
    )
    
    config_5 = RandomForestConfig(
        max_depth=5,
    )

    config_6 = RandomForestConfig(
        n_estimators=200,
        max_depth=5,
    )

    config_7 = RandomForestConfig(
        n_estimators=50,
        max_depth=10,
    )

    config_8 = RandomForestConfig(
        n_estimators=100,
        max_depth=15,
    )

    configs = [
        config_1,
        config_2,
        config_3,
        config_4,
        config_5,
        config_6,
        config_7,
        config_8,
    ]

    for cfg in configs:
        pipe = mlflow_example_pipeline(
            importer=import_data(),
            trainer=trainer(config=cfg),
            evaluator=evaluator(),
        )
        pipe.run()

    print(
        "Now run\n "
        f"mlflow ui --backend-store-uri {get_tracking_uri()}\n"
        "to inspect the experiment runs within the MLFlow UI."
    )
