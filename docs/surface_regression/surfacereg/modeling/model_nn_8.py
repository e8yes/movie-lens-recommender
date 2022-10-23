import tensorflow as tf


def Create_NN_8_Model() -> tf.keras.models.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(2)),
            tf.keras.layers.Dense(units=8, activation=tf.nn.relu),
            tf.keras.layers.Dense(units=1),
        ]
    )

    model.compile(loss=tf.losses.mean_squared_error,
                  optimizer=tf.keras.optimizers.Adam(0.01))
    model.summary()

    return model
