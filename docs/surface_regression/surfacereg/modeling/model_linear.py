import tensorflow as tf


def CreateLinearModel() -> tf.keras.models.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(2)),
            tf.keras.layers.Dense(units=1)
        ]
    )

    model.compile(loss=tf.losses.mean_squared_error,
                  optimizer=tf.keras.optimizers.Adam(0.01))
    model.summary()

    return model
