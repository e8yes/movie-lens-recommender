User and Movie feature data are stored in cassandra database, port localhost.

cassandra use keyspace 'model'.  It has 2 tables  'user'  'movie'

each of them, has column  'id'   'data'

data is the feature of user (movie)

To train the model, 
first need to load the feature data to the cassandra database, 
then set the data_path to the ratings directory

