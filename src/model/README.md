User and Movie feature data are stored in cassandra database, port localhost.

cassandra use keyspace 'model'.  It has 2 tables  'user' table, 'movie' table

each of them, has column  'id','data'   data is the feature of user (movie)

To train the model, 
1.  first need to load the feature data to the cassandra database, 

after start cassandra server, 

CREATE KEYSPACE model WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

use model;

create table movie (id int,data list<float>, PRIMAY KEY(id));
  
create table movie (id int,data list<float>, PRIMAY KEY(id));

Then run load_cassandra.py  (set the path to the dataset file - line 9) which will load data to the cassandra

2. Last, run model.py        (set the path to the dataset/rating)
then set the data_path to the ratings directory,
Model and check point will save under checkpoint_path and model.save() directory
  
In case, can not find dataset from previous session,
  
https://drive.google.com/file/d/1OzfW8QKGQXxwiRJ9cSOhc4DLRyBGGfVs/view  (data_set file for full scale)
  
https://drive.google.com/file/d/1IvzZZFS-zrSQY78D8bf24ZCdPqt75nd7/view?usp=share_link  (reduced data_set, only take part of users rating, which can be trained)
  
unzip, there will be three file content_features,user_features,ratings
  
3. analyze.py will load data from cassandra to spark dataframe, to calculate mse of each user and r square (use prediction from model)
r square currently is not aviable due to not feasible for training the whole user dataset

4. rating_predict.py can print the prediciton rating of given user_id and movie_id  (based on load model)

sample model
https://drive.google.com/drive/folders/1YcZy-3ISL1kWtRqHXXyK6tGCzNo81k5q?usp=share_link      (trained model on reduced dataset)    

python3 rating_predict.py 1 2 'recommendation_model_half_without_matirx/my_model'
first input: user_id   second: movie_id    third: path to model
  
sample output:
user 1 predicted rating on movie 2 is 3.2682076

5. recommendation_generator.py can randomly generate 5 movies as recommendtaion to users

python3 rating_predict.py 1 'recommendation_model_half_without_matirx/my_model'
first input: user_id   second: path to model

sample output:
reccomend 5 movies for user 1:
recommend movie 185677 with predicted rating 4.413979
recommend movie 185515 with predicted rating 4.0680695
recommend movie 187161 with predicted rating 3.9665365
recommend movie 102947 with predicted rating 3.9243002
recommend movie 179427 with predicted rating 3.8663406

