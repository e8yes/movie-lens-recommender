import numpy as np
from zipfile import ZipFile
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from pathlib import Path
import matplotlib.pyplot as plt
import reader
import tensorflow_addons as tfa

EMBEDDING_SIZE = 20

data_path = 'real_data_set1/ratings/tfrecords'  # path to the data_set
test_set = reader.TestSet(data_path)  # test set
train_set = reader.TrainingSet(data_path)  # training set
vaild_set = reader.ValidationSet(data_path)  # vaildation set

user_id = keras.layers.Input(shape=(1,))

# (batch,1)

user_feature = keras.layers.Input(shape=(3,))  # dim 3
movie_feature = keras.layers.Input(shape=(503,))  # dim 503


uers_id1 = tf.reshape(user_id, (-1,))
user_id_embedding = keras.layers.Embedding(
    input_dim=283229, output_dim=7, embeddings_initializer="he_normal",
    input_length=1)(uers_id1)  # embedding user id to 17 dim vector

# concatenate with user_feature (3 dim vector)=> generate 10 dim user_feature vector
user_mergerd = keras.layers.Concatenate(
    axis=1)(
    [user_feature, user_id_embedding])

user_dense = keras.layers.Dense(
    10, activation=keras.activations.relu, use_bias=True)(user_mergerd)


movie_dense1 = keras.layers.Dense(200,  activation=keras.activations.relu, use_bias=True)(
    movie_feature)  # embedding movie feature dim 503 to 200 dim vector
movie_dense2 = keras.layers.Dense(
    10,  activation=keras.activations.relu, use_bias=True)(movie_dense1)


# dot product of    (user_feature)x(movie_feature) to generate rating   of (user,moive) pair
product = tf.keras.layers.Dot(axes=1)([movie_dense2, user_dense])

ouput = 5*tf.keras.activations.sigmoid(product)

#ouput =  tf.keras.layers.Activation(tf.keras.activations.relu)(product)

model10 = keras.models.Model(
    inputs=[user_id, user_feature, movie_feature],
    outputs=ouput)

opt = keras.optimizers.Adam(learning_rate=0.01)

model10.compile(
    optimizer=opt,
    loss='mean_squared_error',
    metrics=[tf.keras.metrics.MeanSquaredError(), tfa.metrics.RSquare()])


checkpoint_path = "training/cp.ckpt"

model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
    filepath=checkpoint_path,
    save_weights_only=False,
    save_freq=1000,
    verbose=1)


model10.summary()


model10.fit(train_set, epochs=1, callbacks=[
            model_checkpoint_callback], validation_data=vaild_set)


model10.save('recommendation_model/my_model')
