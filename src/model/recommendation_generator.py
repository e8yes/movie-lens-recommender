import cassandra_session
import tensorflow as tf
from tensorflow import keras
import sys
import numpy as np
import random 

#[user_id, user_feature, movie_feature]

def rating_predict(uid,mid,model):

    sql1 = 'SELECT * FROM movie where id = ' + str(mid) + ';'
    sql2 = 'SELECT * FROM user where id = ' + str(uid) + ';'

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

    predict = model.predict([(u,user_f,movie_f)])
    return predict



def main(uid,path):
    model = tf.keras.models.load_model(path)
    sql1 = 'SELECT id FROM movie;'
    m_row=cassandra_session.session.execute(sql1)
    s= set()
    for x in m_row:
        s.add(x[0])
    
    count = 0
    dic = dict()
    l = list()

    while(count<100):
        mid = random.sample(s, 1)[0]
        predict = rating_predict(uid,mid,model)
        dic[predict[0][0]] = mid
        l.append(predict[0][0])
        count+=1
    
    l.sort(reverse=True)

    print('reccomend 5 movies for user ' + str(uid))
    for i in range(5):
        print('recommend movie ' + str(dic[l[i]]) + ' with predicted rating ' +str(l[i]))
    


if __name__ == '__main__':
    uid = sys.argv[1]
    path = sys.argv[2]
    main(uid,path)
