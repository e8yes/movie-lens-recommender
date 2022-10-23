import os
import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt

####################################### Data set utils #######################################


def TrainingSet(path: str):
    return np.loadtxt(fname=os.path.join(path, "training.csv"), delimiter=",")


def ValidationSet(path: str):
    return np.loadtxt(fname=os.path.join(path, "validation.csv"), delimiter=",")


def TestSet(path: str):
    return np.loadtxt(fname=os.path.join(path, "test.csv"), delimiter=",")

####################################### Plotting helpers #######################################


def PlotTrainingHistory(history: tf.keras.callbacks.History) -> None:
    plt.plot(history.history["loss"], label="training_loss")
    plt.plot(history.history["val_loss"], label="val_loss")
    plt.xlabel("Epoch")
    plt.ylabel("MSE")
    plt.legend()
    plt.grid(True)
    plt.show()


def PlotPrediction(model: tf.keras.models.Model, data_set_path: str) -> None:
    test_set = TestSet(data_set_path)

    x_test = test_set[:, 0]
    y_true = test_set[:, 1]

    y_pred = model.predict(x=x_test)

    plt.scatter(x=x_test, y=y_true, c="green", s=0.01)
    plt.scatter(x=x_test, y=y_pred, c="red", s=0.01)
    plt.show()


####################################### Model #######################################


def CreateLinearModel() -> tf.keras.models.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1)),
            tf.keras.layers.Dense(units=1),
        ]
    )

    model.compile(loss=tf.losses.mean_squared_error,
                  optimizer=tf.keras.optimizers.Adam(0.001))
    model.summary()

    return model

####################################### Training #######################################


def Train(model: tf.keras.models.Model, data_set_path: str):
    training_set = TrainingSet(data_set_path)
    validation_set = ValidationSet(data_set_path)

    x_train = training_set[:, 0]
    y_train = training_set[:, 1]

    x_val = validation_set[:, 0]
    y_val = validation_set[:, 1]

    history = model.fit(x=x_train,
                        y=y_train,
                        validation_data=(x_val, y_val),
                        epochs=10)

    PlotTrainingHistory(history)

    return model

####################################### Model Parameters #######################################


def PrintModelParameters(model: tf.keras.models.Model) -> None:
    print("Latent W:", model.variables[0].numpy()[0])
    print("Latent B:", model.variables[1].numpy()[0])


def SaveModelParameters(model: tf.keras.models.Model, output_path: str) -> None:
    model.save(filepath=output_path)

####################################### Main #######################################


if __name__ == "__main__":
    linear_model = CreateLinearModel()
    linear_model = Train(model=linear_model, data_set_path="./data_set")
    PlotPrediction(model=linear_model, data_set_path="./data_set")
    PrintModelParameters(model=linear_model)
    SaveModelParameters(model=linear_model, output_path="./linear_model")
