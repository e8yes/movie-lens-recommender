import tensorflow as tf

from surfacereg.modeling.model_linear import CreateLinearModel
from surfacereg.modeling.model_nn_8 import Create_NN_8_Model
from surfacereg.modeling.model_nn_32_32 import Create_NN_32_32_Model
from surfacereg.modeling.model_nn_128_64_32_32 import Create_NN_128_64_32_32_Model
from surfacereg.utils.plot import PlotTrainingHistory
from surfacereg.utils.reader import TrainingSet
from surfacereg.utils.reader import ValidationSet

MODEL_LINEAR = "linear"
MODEL_NN_8 = "nn_8"
MODEL_NN_32_32 = "nn_32_32"
MODEL_NN_128_64_32_32 = "nn_128_64_32_32"


def TrainModel(model_type: str,
               data_set_path: str) -> tf.keras.models.Model:
    """_summary_

    Args:
        model_type (str): _description_
        data_set_path (str): _description_

    Returns:
        tf.keras.models.Model: _description_
    """
    model: tf.keras.models.Model = None
    epochs = 0

    if model_type == MODEL_LINEAR:
        model = CreateLinearModel()
        epochs = 60
    elif model_type == MODEL_NN_8:
        model = Create_NN_8_Model()
        epochs = 100
    elif model_type == MODEL_NN_32_32:
        model = Create_NN_32_32_Model()
        epochs = 100
    elif model_type == MODEL_NN_128_64_32_32:
        model = Create_NN_128_64_32_32_Model()
        epochs = 100

    training_set = TrainingSet(data_set_path)
    validation_set = ValidationSet(data_set_path)

    history = model.\
        fit_generator(generator=training_set.batch(100),
                      validation_data=validation_set.batch(1000),
                      epochs=epochs)

    PlotTrainingHistory(history)

    return model
