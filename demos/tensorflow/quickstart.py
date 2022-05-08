import numpy as np
import tensorflow as tf

from coalescenceml.pipeline import pipeline
from coalescenceml.step import step, Output
from coalescenceml.integrations.constants import TENSORFLOW
from coalescenceml.integrations.tensorflow.step import (
    TFClassifierConfig,
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

    print(y_test.shape)

    return X_train, y_train, X_test, y_test


@step
def tf_trainer(X_train: np.ndarray, y_train: np.ndarray, x_val: np.ndarray, y_val:np.ndarray) -> Output(model=tf.keras.Model):
    model = tf.keras.Sequential(
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
        epochs=10,
        batch_size=32,
        validation_data=(x_val, y_val),
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
    model = trainer(X_train, y_train, X_test, y_test)
    mse = evaluator(model, X_test, y_test)


if __name__ == '__main__':
    tf_train_config = TFClassifierConfig(
        layers = [10,],
        input_shape = (28, 28),
        num_classes=10,
        epochs=10,
        batch_size=32,
        learning_rate=1e-3,
    )

    pipe_1 = sample_pipeline(
            importer=importer(),
            trainer=TFClassifierTrainStep(config=tf_train_config),
            evaluator=tf_evaluator()
        )
    pipe_1.run()


    pipe_2 = sample_pipeline(
        importer=importer(),
        trainer=tf_trainer(),
        evaluator=tf_evaluator(),
    )
    pipe_2.run()
