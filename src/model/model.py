import numpy as np
from zipfile import ZipFile
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from pathlib import Path
import matplotlib.pyplot as plt
import reader

EMBEDDING_SIZE = 20 
num_users = 280000
num_movies = 60000


data_path = 'fake_data_set_reduce/ratings'                #path to the data_set
test_set = reader.TrainingSet(data_path)                  #training set
train_set = reader.TrainingSet(data_path)                 #test set
vaild_set = reader.ValidationSet(data_path)               #vaildation set



user_id = keras.layers.Input(shape=(1, ))
user_feature = keras.layers.Input(shape=(3,))                  #dim 3
movie_feature = keras.layers.Input(shape=(1506,))              #dim 1506
movie_id = keras.layers.Input(shape=(1, ))

user_id_dense = keras.layers.Dense(17,  activation=keras.activations.sigmoid, use_bias=True)(user_id)             #embedding user id to 17 dim vector
user_mergerd=  keras.layers.Concatenate(axis=1)([user_feature, user_id_dense])                                    #concatenate with user_feature (3 dim vector)=> generate 20 dim user_feature vector


movie_dense1 = keras.layers.Dense(200,  activation=keras.activations.sigmoid, use_bias=True)(movie_feature)       #embedding movie feature dim 1506 to 20 dim vector
movie_dense2 = keras.layers.Dense(20,  activation=keras.activations.sigmoid, use_bias=True)(movie_dense1)

 
product =  tf.keras.layers.Dot(axes=1)([movie_dense2, user_mergerd])                                               #dot product of    (user_feature)x(movie_feature) to generate rating   of (user,moive) pair

ouput =  tf.keras.activations.sigmoid(product)

model10 = keras.models.Model(inputs=[user_id, user_feature, movie_feature,movie_id], outputs=ouput)

opt = keras.optimizers.Adam(learning_rate=0.001)
model10.compile(
    optimizer =opt,
    loss = 'mean_squared_error'
)

model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
    filepath='/model_checkpoint',
    save_weights_only=True,
    monitor='val_accuracy',
    mode='max',
    save_freq =1000,
    save_best_only=True)


model10.fit(train_set, epochs=1,validation_data=vaild_set)

model10.save('saved_model')
