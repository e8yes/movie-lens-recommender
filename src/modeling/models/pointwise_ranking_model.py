import os
import numpy as np
import tensorflow as tf
from typing import List

USER_EMBEDDING_LAYER = "user_embedding"
CONTENT_EMBEDDING_LAYER = "content_embedding"
POINTWISE_RANKING_OUTPUT = "rating"


class PointwiseRankingModel:
    """It encapsulates all operations one can do to a pointwise ranking model.
    A pointwise ranking model gives the prediction of
        Pr(like(content[i]) | user[j]).
    It also creates user and model embeddings.
    """

    def __init__(self,
                 name: str,
                 model: tf.keras.Model,
                 sparse_content_features: bool,
                 sparse_user_features: bool,
                 user_embedding_dim: int,
                 content_embeding_dim: int) -> None:
        """Constructs a ranking model.

        Args:
            name (str): The name of the model architecture.
            model (tf.keras.Model): The model architecture.
            sparse_content_features (bool): Does the model uses sparse content
                features? It means that it only uses the content ID to
                represent a piece of content.
            sparse_user_features (bool): Does the model uses sparse user
                features? It means that it only uses the user ID to represent
                a user.
            user_embedding_dim (int): The dimension of user embeddings it's
                going to generate.
            content_embedding_dim (int): The dimension of the content
                embedding it's going to generate.
        """
        self.name = name
        self.model = model
        self.sparse_content_features = sparse_content_features
        self.sparse_user_features = sparse_user_features
        self.user_embedding_dim = user_embedding_dim
        self.content_embedding_dim = content_embeding_dim

    def Fit(self,
            train_set: tf.data.Dataset,
            valid_set: tf.data.Dataset,
            checkpoint_path: str,
            epoch: int):
        """Fits the model with the given data set.

        Args:
            train_set (tf.data.Dataset): Iterator over the training set.
            valid_set (tf.data.Dataset): Iterator over the validation set.
            checkpoint_path (str): Path where it can store checkpoint models.
            epoch (int): The number of epochs to train for.
        """
        model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
            filepath=os.path.join(checkpoint_path, self.name),
            save_weights_only=True,
            save_freq=10000,
            verbose=1)

        self.model.fit(
            train_set,
            validation_data=valid_set,
            callbacks=[model_checkpoint_callback],
            epochs=epoch)

    def Predict(self, data_set: tf.data.Dataset) -> List[np.ndarray]:
        """Generates predictions with model over the input data set.

        Args:
            data_set (tf.data.Dataset): Iterator over the independent
                variables (features).

        Returns:
            List[np.ndarray]: An array containing vectors of
                0: Pr(like(content[i]) | user[j])
                1: user embeddings,
                2: content embeddings.
        """
        return self.model.predict(data_set)

    def Load(self, model_path: str):
        """Loads saved model parameters from the specified file path.

        Args:
            model_path (str): File path to the saved model parameters.
        """
        self.model.load_weights(filepath=os.path.join(model_path, self.name))

    def Save(self, output_path: str):
        """Saves model parameters to the specified file path.

        Args:
            output_path (str): File path to which the model parameters are
                going to be saved.
        """
        self.model.save_weights(filepath=os.path.join(output_path, self.name))
