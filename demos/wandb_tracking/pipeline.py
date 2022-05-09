import numpy as np
import tensorflow as tf
import wandb
from wandb.keras import WandbCallback

from coalescenceml.integrations.contants import TENSORFLOW, WANDB
from coalescenceml.integrations.wandb.wandb_step_decorator import enable_wandb
from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step


class TFTrainerConfig(BaseStepConfig):
    """Trainer parameters."""

    epochs: int = 1
    lr: float = 1e-3


@step
def import_mnist() -> Output(
    x_train=np.ndarray, y_train=np.ndarray,
    x_test=np.ndarray, y_test=np.ndarray,
):
    (x_train, y_train), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
    return x_train, y_train, x_test, y_test


@step
def normalize(
    x_train: np.ndarray, x_test: np.ndarray,
) -> Output(x_train_normalized=np.ndarray, x_test_normalized=np.ndarray,):
    x_train_normalized = x_train / 255.0
    x_test_normalized = x_test / 255.0
    return x_train_normalized, x_test_normalized


@enable_wandb
@step
def tf_trainer(
    config: TFTrainerConfig,
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_val: np.ndarray,
    y_val: np.ndarray,
)-> Output(model=tf.keras.Model):
    model = tf.keras.Sequential(
        [
            tf.keras.layers.Flatten(input_shape=(28,28)),
            tf.keras.layers.Dense(10),
        ]
    )

    model.compile(
        optimizer=tf.keras.optimizers.SGD(learning_rate=config.lr, momentum=0.9),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"]
    )

    model.fit(
        x_train,
        y_train,
        epochs=config.epochs,
        validation_data=(x_val, y_val),
        callbacks=[
            WandbCallback(
                log_evaluation=True,
                validation_steps=16,
                validation_data=(x_val, y_val)
            )
        ],
    )

    return model


@enable_wandb
@step
def tf_evaluation(
    model: tf.keras.Model,
    x_val: np.ndarray,
    y_val: np.ndarray,
) -> Output(accuracy=float):
    _, val_acc = model.evaluate(x_val, y_val)
    wandb.log({"val_accuracy": val_acc})
    return val_acc


@pipeline(enable_cache=False, required_integrations=[TENSORFLOW, WANDB])
def wandb_sample_pipeline(
    importer,
    normalize,
    trainer,
    evaluator,
):
    x_train, y_train, x_test, y_test = importer()
    x_train_normalized, x_test_normalized = normalize(x_train, x_test)
    model = trainer(x_train, y_train, x_test, y_test)
    val_acc = evaluator(model, x_test, y_test)


if __name__ == '__main__':
    pipe_1 = wandb_sample_pipeline(
        import_mnist(),
        normalize(),
        tf_trainer(config=TFTrainerConfig(epochs=5, lr=3e-4)),
        tf_evaluation(),
    )
    pipe_1.run()


    pipe_2 = wandb_sample_pipeline(
        import_mnist(),
        normalize(),
        tf_trainer(config=TFTrainerConfig(epochs=5, lr=1e-4)),
        tf_evaluation(),
    )
    pipe_2.run()
