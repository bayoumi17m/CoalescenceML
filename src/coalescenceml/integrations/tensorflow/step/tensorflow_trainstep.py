import typing
from typing import Any, Dict, List, Optional, Tuple, Type

import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from coalescenceml.step import BaseStep
from coalescenceml.step import BaseStepConfig

class TFClassifierConfig(BaseStepConfig):
    layers: typing.List
    input_shape: Tuple[int, ...] = (28,28) # MNIST Size
    num_classes: int = 2
    learning_rate: float = 1e-3
    epochs: int = 10
    batch_size: int = 32


class TFClassifierTrainStep(BaseStep):
    """ """
    def entrypoint(
        self,
        config: TFClassifierConfig,
        x_train: np.ndarray,
        y_train: np.ndarray,
        x_validation: np.ndarray = np.array([]),
        y_validation: np.ndarray = np.array([]),
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
            optimizer=tf.keras.optimizers.SGD(
                learning_rate=config.learning_rate,
                momentum=0.9,
            ),
            metrics=["accuracy"],
        )

        if len(x_validation) == 0:
            checkpoint_callback = ModelCheckpoint(
                filepath="best_model.hdf5",
                monitor="val_accuracy",
                save_best_only=True,
                save_weights_only=True,
            )
            early_stop = EarlyStopping(
                monitor="val_accuracy",
                mode="max",
                patience=20,
            )
        else:
            checkpoint_callback = ModelCheckpoint(
                filepath="best_model.hdf5",
                monitor="accuracy",
                save_best_only=True,
                save_weights_only=True,
            )
            early_stop = EarlyStopping(
                monitor="accuracy", mode="max", patience=20,
            )

        model.fit(
            x_train,
            y_train,
            batch_size=config.batch_size,
            epochs=config.epochs,
            callbacks=[checkpoint_callback, early_stop],
            validation_data=((x_validation, y_validation) if len(x_validation) > 0 else None),
        )

        model.load_weights("best_model.hdf5")

        return model
