import mlflow
import numpy as np
import tensorflow as tf

from coalescenceml.integrations.constants import MLFLOW, TENSORFLOW
from coalescenceml.integrations.mlflow.mlflow_step_decorator import enable_mlflow
from coalescenceml.pipelines import pipeline
from coalescenceml.step import BaseStepConfig, Output, step


@step
def import_mnist() -> Output(
    x_train=np.ndarray, y_train=np.ndarray,
    x_test=np.ndarray, y_test=np.ndarray,
):
    (
        (x_train, y_train),
        (x_test, y_test),
    ) = tf.keras.datasets.mnist.load_data()
    return x_train, y_train, x_test, y_test


@step
def normalize(
    x_train: np.ndarray, x_test: np.ndarray,
) -> Output(
    x_train_normalized=np.ndarray,
    x_test_normalized=np.ndarray,
):
    x_train_normalized = x_train / 255.
    x_test_normalized = x_test / 255.
    return x_train_normalized, x_test_normalized


class TFTrainerConfig(BaseStepConfig):
    epochs: int = 5
    batch_size: int = 32
    lr: float = 1e-3
    momentum: float = 0.9


@enable_mlflow
@step
def tf_trainer(
    config: TFTrainerConfig,
    x_train: np.ndarray,
    y_train: np.ndarray,
) -> Output(model=tf.keras.Model):
    model = tf.keras.Sequential(
        [
            tf.keras.layers.Flatten(input_shape=(28,28)),
            tf.keras.layers.Dense(512, activation='relu'),
            tf.keras.layers.Dense(10, activation='linear'),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.SGD(
            learning_rate=config.lr,
            momentum=config.momentum,
        ),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )

    mlflow.tensorflow.autolog()
    model.fit(
        x_train,
        y_train,
        epochs=config.epochs,
        batch_size=config.batch_size,
    )

    return model


@enable_mlflow
@step
def tf_evaluator(
    model: tf.keras.Model,
    x_test: np.ndarray,
    y_test: np,ndarray,
) -> Output(acc=float):
    _, test_acc = model.evaluate(x_test, y_test)
    mlflow.log_metrics({"test_acc": test_acc})
    return test_acc


@pipeline(enable_cache=False, required_integrations=[MLFLOW, TENSORFLOW])
def mlflow_example_pipeline(
    importer,
    normalize,
    trainer,
    evaluator,
):
    x_train, y_train, x_test, y_test = import_mnist()
    x_tr_normalized, x_te_normalized = normalize(x_train, x_test)
    model = trainer(x_tr_normalized, y_train)
    evaluator(model, x_tr_normalized, y_test)


def get_tracking_uri() -> str:
    from coalescenceml.directory import Directory

    tracker = Directory().active_stack.experiment_tracker

    return tracker.get_tracking_uri()

if __name__ == '__main__':
    config_1 = TFTrainerConfig(
        lr=1e-1,
    )

    config_2 = TFTrainerConfig(
        lr=1e-2,
    )

    config_3 = TFTrainerConfig(
        lr=1e-3,
    )

    config_4 = TFTrainerConfig(
        batch_size=64,
    )
    
    config_5 = TFTrainerConfig(
        batch_size=32,
    )

    config_6 = TFTrainerConfig(
        batch_size=16,
    )

    config_7 = TFTrainerConfig(
        epochs=10,
    )

    config_8 = TFTrainerConfig(
        epochs=20,
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
            importer=import_mnist(),
            normalize=normalize(),
            trainer=tf_trainer(config=config_1),
            evaluator=tf_evaluator(),
        )
        pipe.run()

    print(
        "Now run\n "
        f"mlflow ui --backend-store-uri {get_tracking_uri()}\n"
        "to inspect the experiment runs within the MLFlow UI."
    )
