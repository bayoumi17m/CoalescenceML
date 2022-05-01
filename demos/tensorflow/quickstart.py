import numpy as np
import tensorflow as tf

from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.constants import TENSORFLOW
from coalescenceml.integrations.tensorflow.step import (
    TFClassifierTrainConfig,
    TFClassifierTrainStep,
)

@step
def importer() -> Output(
    X_train=np.ndarray, y_train=np.ndarray, X_test=np.ndarray, y_test=np.ndarray
):
    (
        (X_train, y_train),
        (X_test, y_test),
    ) = tf.keras.datasets.mnist.load_data()

    return X_train, y_train, X_test, y_test


@step
def trainer(X_train: np.ndarray, y_train: np.ndarray) -> Output(model=tf.keras.Model):
    model = tf.keras.sequential(
        [
            tf.keras.layers.Flatten(input_shape=(28,28)),
            tf.keras.layers.Dense(10, activation='relu'),
            tf.keras.layers.Dense(10),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.SGD(learning_rate=1e-3, momentum=0.9),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )

    model.fit(
        X_train,
        y_train,
        epochs=5,
    )

    return model


@step
def tf_evaluator(
    model: tf.keras.Model,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> Output(test_acc=float):
    _, test_acc = model.evaluate(X_test, y_test)
    return test_acc


@pipeline(required_integrations=[TENSORFLOW])
def sample_pipeline(importer, trainer, evaluator):
    X_train, y_train, X_test, y_test = importer()
    model = trainer(X_train, y_train)
    mse = evaluator(model, X_test, y_test)


if __name__ == '__main__':
    tf_train_config = TFClassifierTrainConfig(
        layers = [8, 4],
        hyperparams = {
          "epochs": 256, "batch_size": 10
        }
    )

    pipe = sample_pipeline(
            importer=importer(),
            trainer=TFClassifierTrainStep(TFClassifierTrainConfig),
            evaluator=evaluator()
        )
    pipe.run()
