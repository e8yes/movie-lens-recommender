import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf


def PlotTargetDistribution(z_true: np.ndarray,
                           z_pred: np.ndarray) -> None:
    fig = plt.figure()

    axes = fig.subplots(nrows=1, ncols=2)

    axes[0].hist(x=z_true, bins=50, color="blue")
    axes[1].hist(x=z_pred, bins=50, color="red")

    axes[0].set_title("ground_truth")
    axes[1].set_title("prediction")

    plt.show()


def PlotSurfaceSamples(x: np.ndarray,
                       z_true: np.ndarray,
                       z_pred: np.ndarray) -> None:
    fig = plt.figure()
    ax = fig.add_subplot(projection="3d")

    ax.scatter(x[:, 0],
               x[:, 1],
               z_true,
               s=0.01,
               c="blue")

    if z_pred is not None:
        ax.scatter(x[:, 0],
                   x[:, 1],
                   z_pred,
                   s=0.01,
                   c="red")

    ax.set_xlabel("x1")
    ax.set_ylabel("x2")
    ax.set_zlabel("z")

    plt.show()


def PlotTrainingHistory(history: tf.keras.callbacks.History) -> None:
    plt.plot(history.history["loss"], label="training_loss")
    plt.plot(history.history["val_loss"], label="val_loss")
    plt.xlabel("Epoch")
    plt.ylabel("MSE")
    plt.legend()
    plt.grid(True)
    plt.show()
