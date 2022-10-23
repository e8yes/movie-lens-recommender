import tensorflow as tf


def Create_NN_128_64_32_32_Model() -> tf.keras.models.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(2)),
            tf.keras.layers.Dense(units=128,
                                  kernel_regularizer=tf.keras.regularizers.L2(),
                                  activation=tf.keras.activations.relu),
            tf.keras.layers.Dense(units=64,
                                  kernel_regularizer=tf.keras.regularizers.L2(),
                                  activation=tf.keras.activations.relu),
            tf.keras.layers.Dense(units=32,
                                  kernel_regularizer=tf.keras.regularizers.L2(),
                                  activation=tf.keras.activations.relu),
            tf.keras.layers.Dense(units=32,
                                  kernel_regularizer=tf.keras.regularizers.L2(),
                                  activation=tf.keras.activations.relu),
            tf.keras.layers.Dense(units=1),
        ]
    )

    model.compile(loss=tf.losses.mean_squared_error,
                  optimizer=tf.keras.optimizers.Adam(0.01))
    model.summary()

    return model
