import typing
from typing import Any, Dict, Tuple, Type

import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from coalescenceml.step import BaseStep
from coalescenceml.step import BaseStepConfig

class TFClassifierTrainConfig(BaseStepConfig):
    layers: typing.List
    input_shape: Tuple[int, ...] = (28,28) # MNIST Size
    num_classes: int = 2
    
    learning_rate: float = 1e-3
    optimizer: Type[
        tf.keras.opimtizers.Optimizer
    ] = tf.keras.optimizers.SGD
    optimizer_hyperparams: Dict[str, Any] = {}

    metrics: List[tf.keras.metrics.Metric] = [
        tf.keras.metrics.Accuracy(),
    ]
    epochs: int = 10
    batch_size: int = 32


class TFClassifierStep(BaseStep):
    def entrypoint(
        self,
        config: TFClassifierTrainConfig,
        x_train: np.ndarray,
        y_train: np.ndarray,
        x_validation: Optional[np.ndarray] = None,
        y_validation: Optional[np.ndarray] = None,
    ) -> tf.keras.Model:
        # Possibly import optimizer and change its hyperparameters

        model = Sequential()
        model.add(tf.keras.layers.InputLayer(input_shape=config.input_shape))
        model.add(tf.keras.layers.Flatten())

        last_layer = config.num_classes if config.num_classes > 2 else 1
        # Construct layers
        for i, layer_size in enumerate(config.layers):
            model.add(tf.keras.layers.Dense(layer_size, activation='relu'))

        model.add(Dense(last_layer, activation='linear'))

        if last_layer == 1:
            loss = tf.keras.losses.BinaryCrossentropy(from_logits=True)
        else:
            loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)

        model.compile(
            loss=loss,
            optimizer=config.optimizer(
                learning_rate=config.learning_rate,
                **config.optimizer_params,
            ),
            metrics=config.metrics,
        )

        if len(x_validation) == 0:
            callback_a = ModelCheckpoint(
                filepath="my_best_model.hdf5",
                monitor="loss",
                save_best_only=True,
                save_weights_only=True,
            )
            callback_b = EarlyStopping(
                monitor="loss", mode="min", patience=20, verbose=1
            )

            # Train model
            model.fit(
                x_train,
                y_train,
                callbacks=[callback_a, callback_b],
                verbose=0
            )
        else:
            callback_a = ModelCheckpoint(
                filepath="my_best_model.hdf5",
                monitor="val_loss",
                save_best_only=True,
                save_weights_only=True,
            )
            callback_b = EarlyStopping(
                monitor="val_loss", mode="min", patience=20, verbose=1
            )

            # Train model
            model.fit(
                x_train,
                y_train,
                validation_data={x_validation, y_validation} ** hyperparams,
                callbacks=[callback_a, callback_b],
                verbose=0,
            )

        model.load_weights("my_best_model.hdf5")

        return model
