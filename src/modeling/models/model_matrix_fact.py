import tensorflow as tf
import tensorflow_addons as tfa

from src.modeling.models.pointwise_ranking_model import USER_EMBEDDING_LAYER
from src.modeling.models.pointwise_ranking_model import CONTENT_EMBEDDING_LAYER
from src.modeling.models.pointwise_ranking_model import \
    POINTWISE_RANKING_OUTPUT
from src.modeling.models.pointwise_ranking_model import PointwiseRankingModel


def CreateMatrixFactorizationModel(
        user_count: int,
        content_count: int,
        user_latent_dim: int,
        content_latent_dim: int) -> PointwiseRankingModel:
    """It factorizes the user-content rating matrix into:
        RATINGS = sigmoid(w*phi_u(USERS)*phi_c(CONTENTS) + b)
    In particular, it doesn't utilize any extracted user or content features.

    Args:
        user_count (int): The total number of users in the data set.
        content_count (int): The total number of pieces of contents in the
            data set.
        user_latent_dim (int): The number of dimensions to use to represent a
            user.
        content_latent_dim (int): The number of dimensions to use to represent
            a piece of content.

    Returns:
        PointwiseRankingModel: An untrained pointwise ranking model.
    """
    user_idx = tf.keras.Input(shape=(1,))
    user_features = tf.keras.Input(shape=(3,))
    content_idx = tf.keras.Input(shape=(1,))
    content_feautures = tf.keras.Input(shape=(503,))

    user_idx_1d = tf.reshape(user_idx, (-1,))
    user_repr = tf.keras.layers.Embedding(
        input_dim=user_count,
        output_dim=user_latent_dim,
        input_length=1,
        name=USER_EMBEDDING_LAYER)(user_idx_1d)
    user_repr2 = tf.keras.layers.Dense(
        units=user_latent_dim,
        activation=tf.keras.activations.relu)(user_repr)

    content_idx_1d = tf.reshape(content_idx, (-1,))
    content_repr = tf.keras.layers.Embedding(
        input_dim=content_count,
        output_dim=content_latent_dim,
        input_length=1,
        name=CONTENT_EMBEDDING_LAYER)(content_idx_1d)
    content_repr2 = tf.keras.layers.Dense(
        units=content_latent_dim,
        activation=tf.keras.activations.relu)(content_repr)

    compressed_dim = min(user_latent_dim, content_latent_dim)
    user_repr3 = tf.keras.layers.Dense(
        units=compressed_dim,
        activation=tf.keras.activations.relu)(user_repr2)
    content_repr3 = tf.keras.layers.Dense(
        units=compressed_dim,
        activation=tf.keras.activations.relu)(content_repr2)

    u_dot_c = tf.keras.layers.Dot(axes=1)([user_repr3, content_repr3])
    rating = tf.keras.layers.Dense(
        units=1,
        activation=tf.keras.activations.sigmoid,
        name=POINTWISE_RANKING_OUTPUT)(u_dot_c)

    model = tf.keras.models.Model(
        inputs=[user_idx, user_features, content_idx, content_feautures],
        outputs=[rating, user_repr, content_repr])

    model.compile(
        optimizer=tf.keras.optimizers.Adam(),
        loss={POINTWISE_RANKING_OUTPUT: tf.keras.losses.MSE},
        metrics={POINTWISE_RANKING_OUTPUT: tfa.metrics.RSquare()})
    model.summary()

    return PointwiseRankingModel(
        name="PointwiseMatrixFactorization_" +
        str(user_latent_dim) +
        "_" +
        str(content_latent_dim),
        model=model, sparse_content_features=True, sparse_user_features=True,
        user_embedding_dim=user_latent_dim,
        content_embeding_dim=content_latent_dim)
