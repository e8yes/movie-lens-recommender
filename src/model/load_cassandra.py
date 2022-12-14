import sys
assert sys.version_info >= (3, 5) 
import os
from pyspark.sql import SparkSession, functions, types
import numpy as np

def main():
    
    path = 'real_data_set1'
    user_df = spark.read.parquet(path +'user_features')
    movie_df = spark.read.parquet(path +'content_features')
    
   
    user_df= user_df.withColumn("data",functions.array("avg_rating", "rating_count",'tagging_count')).select('id','data')
    #user_df= user_df.withColumn('data',functions.concat("rating_feature",'profile')).select('id','data','rating_feature')

    #user_df dataframe of user_feature, in a high dimensions vector  


    user_df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='user', keyspace="model").save()
  

    movie_df = spark.read.parquet('real_data_set1/content_features')
    movie_df= movie_df.withColumn("feature",functions.array("avg_rating", "rating_count",'budget','runtime','release_year','tmdb_avg_rating',\
                                                            'tmdb_vote_count'))
    movie_df= movie_df.withColumn("data",functions.concat("feature",'genres','languages','cast_composition','crew_composition','summary','tag','keyword')).select('id','data')

    #movie_feature has 503 dimensions
    #facing problem when load large to cassandra, try to do batch by batch
    
    i =10000
    while(i<200000):
        curt = movie_df.where(movie_df['id']<i)
        movie_df = movie_df.where(movie_df['id']>=i)
        curt.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='movie', keyspace="model").save()
        i =i+1000
    


if __name__ == '__main__':
    spark = SparkSession.builder.appName('tf_record_reader').config("spark.jars","third_party/spark-cassandra-connector_2.12-3.2.0.jar").\
                                                        config('spark.sql.extensions','com.datastax.spark.connector.CassandraSparkExtensions').\
                                                        getOrCreate()
    assert spark.version >= '2.3' 
    spark.sparkContext.setLogLevel('WARN')
    main()
