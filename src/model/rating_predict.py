import cassandra_session
import tensorflow as tf
from tensorflow import keras
import sys
import numpy as np


#[user_id, user_feature, movie_feature]


def main(uid,mid):
    new_model = tf.keras.models.load_model('recommendation_model_half_without_matirx/my_model')

    sql1 = 'SELECT * FROM movie where id = ' + mid + ';'
    sql2 = 'SELECT * FROM user where id = ' + uid + ';'

    m_row=cassandra_session.session.execute(sql1)
    u_row=cassandra_session.session.execute(sql2)

    for x in u_row:
        value = x[1]
        break

    for x in m_row:
        value2 = x[1]
        break

    user_array = np.asarray(value).astype('float32')
    m_array = np.asarray(value2).astype('float32')
    user_f = tf.convert_to_tensor([user_array])
    u = tf.convert_to_tensor([[np.asarray(uid).astype('int')]])
    movie_f =tf.convert_to_tensor([m_array])


    predict = new_model.predict([(u,user_f,movie_f)])
    print(predict)


if __name__ == '__main__':
    uid = sys.argv[1]
    mid = sys.argv[2]
    main(uid,mid)
