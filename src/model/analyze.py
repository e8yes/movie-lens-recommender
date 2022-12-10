import sys
assert sys.version_info >= (3, 5) 
import os
from pyspark.sql import SparkSession, functions, types
import numpy as np
from pyspark.sql import functions as F

def main():
    read_path = 'real_data_set1/ratings/parquet/test'
    df = spark.read.parquet(read_path)

    print(df.agg({"user_id":"max"}).first()[0])
    print(df.agg({"content_id":"max"}).first()[0])

 
    df = df.withColumn('y_hat',functions.lit(2.5))

  
    average = df.groupBy("user_id").agg(F.mean('rating'))
    average.show()



    df = df.join(average,df['user_id'] == average['user_id']).drop(df['user_id'])

    df = df.withColumn('mse', (df['rating']-df['avg(rating)'])* (df['rating']-df['avg(rating)']))

    df = df.withColumn('differen_square', (df['rating']-df['y_hat'])* (df['rating']-df['y_hat'])).select('user_id','mse','differen_square')

    df = df.groupby('user_id').agg(F.sum('mse'),F.sum('differen_square'))

    df.show()

    df = df.withColumn('R_square',  functions.lit(1)-(df['sum(differen_square)']/df['sum(mse)']) )
    
    #calculated mse for each user
    
    df.show()
    

if __name__ == '__main__':
    spark = SparkSession.builder.appName('analyze').getOrCreate()
    assert spark.version >= '2.3' 
    spark.sparkContext.setLogLevel('WARN')
    main()
