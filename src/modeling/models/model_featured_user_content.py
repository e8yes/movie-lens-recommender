import tensorflow as tf
import tensorflow_addons as tfa

from src.modeling.models.pointwise_ranking_model import USER_EMBEDDING_LAYER
from src.modeling.models.pointwise_ranking_model import CONTENT_EMBEDDING_LAYER
from src.modeling.models.pointwise_ranking_model import \
    POINTWISE_RANKING_OUTPUT
from src.modeling.models.pointwise_ranking_model import PointwiseRankingModel


def CreateFeaturedUserContentModel(
        user_count: int, user_latent_dim: int) -> PointwiseRankingModel:
    """It uses both user and content features to predict the rating target. In
    addition, it "generates" extra user features, which also serves as user
    embeddings, to explain the target variable.

    Args:
        user_count (int): The total number of users in the data set.
        user_latent_dim (int): The number of dimensions to use to represent a
            user.

    Returns:
        PointwiseRankingModel: An untrained pointwise ranking model.
    """
    user_idx = tf.keras.Input(shape=(1,))
    user_features = tf.keras.Input(shape=(3,))
    content_idx = tf.keras.Input(shape=(1,))
    content_feautures = tf.keras.Input(
        shape=(503,), name=CONTENT_EMBEDDING_LAYER)

    user_idx_1d = tf.reshape(user_idx, (-1,))
    user_repr = tf.keras.layers.Embedding(
        input_dim=user_count,
        output_dim=user_latent_dim,
        input_length=1,
        name=USER_EMBEDDING_LAYER)(user_idx_1d)

    features = tf.keras.layers.Concatenate(axis=1)(
        [user_repr, user_features, content_feautures])

    h1 = tf.keras.layers.Dense(
        units=200,
        activation=tf.keras.activations.relu)(features)

    h2 = tf.keras.layers.Dense(
        units=100,
        activation=tf.keras.activations.relu)(h1)
    h2_dropped = tf.keras.layers.Dropout(rate=0.1)(h2)

    z3 = tf.keras.layers.Dense(units=100)(h2_dropped)
    h3 = tf.keras.activations.relu(z3 + h2_dropped)

    z4 = tf.keras.layers.Dense(units=100)(h3)
    h4 = tf.keras.activations.relu(z4 + h3)

    z5 = tf.keras.layers.Dense(units=100)(h4)
    h5 = tf.keras.activations.relu(z5 + h4)

    z6 = tf.keras.layers.Dense(units=100)(h5)
    h6 = tf.keras.activations.relu(z6 + h5)

    rating = tf.keras.layers.Dense(
        units=1,
        activation=tf.keras.activations.sigmoid,
        name=POINTWISE_RANKING_OUTPUT)(h6)

    model = tf.keras.models.Model(
        inputs=[user_idx, user_features, content_idx, content_feautures],
        outputs=[rating, user_repr, content_feautures])

    model.compile(
        optimizer=tf.keras.optimizers.Adam(),
        loss={POINTWISE_RANKING_OUTPUT: tf.keras.losses.MSE},
        metrics={POINTWISE_RANKING_OUTPUT: tfa.metrics.RSquare()})
    model.summary()

    return PointwiseRankingModel(
        name="PointwiseFeaturedUserContent_" + str(user_latent_dim),
        model=model,
        sparse_content_features=False,
        sparse_user_features=True,
        user_embedding_dim=user_latent_dim,
        content_embeding_dim=503)
