import typing
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from keras.callbacks import EarlyStopping, ModelCheckpoint
from coalescenceml.step import BaseStep

# https://www.youtube.com/watch?v=cJ3oqHqRBF0
class TFClassifierTrainStep(BaseStep):
    # Hyperparameters used in video: epochs 256, batch_size 10
    # Note to users: layers parameter shouldn't have 1 as last - it is automatically done
    def entrypoint(
        self,
        x_train: np.ndarray,
        y_train: np.ndarray,
        layers: typing.List,
        hyperparams: typing.Dict = {},
        x_validation: np.ndarray = np.array([]),
        y_validation: np.ndarray = np.array([]),
    ) -> tf.keras.Model:
        # Possibly import optimizer and change its hyperparameters

        model = Sequential()

        # Construct layers
        for x in range(len(layers)):
            if x == 0:
                model.add(
                    Dense(
                        layers[x],
                        input_dim=len(x_train[0, :]),
                        activation="relu",
                    )
                )
            else:
                model.add(Dense(layers[x], activation="relu"))
        model.add(Dense(1, activation="sigmoid"))

        model.compile(
            loss="binary_crossentropy", optimizer="adam", metrics=["accuracy"]
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
                **hyperparams,
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
