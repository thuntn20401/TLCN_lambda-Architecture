from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, col,udf,split,explode,desc,rank
from pyspark.sql.functions import avg, current_timestamp, format_number
from pyspark.sql.window import Window


cluster_cassandra = ['cassandra:9042']
sparkCassandra = SparkSession \
    .builder \
    .appName("test") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()


query_users = sparkCassandra.read\
    .format("org.apache.spark.sql.cassandra")\
    .option("spark.cassandra.connection.host", "cassandra")\
    .option("spark.cassandra.connection.port", "9042") \
    .options(keyspace="stackoverflow", table="toptags")\
    .load()


print("====DONE===!")


query_questions = sparkCassandra.read\
    .format("org.apache.spark.sql.cassandra")\
    .option("spark.cassandra.connection.host", "cassandra")\
    .option("spark.cassandra.connection.port", "9042") \
    .options(keyspace="stackoverflow", table="realtimequestions")\
    .load()



print("====DONE===!")

# userid 	TEXT , questions TEXT
df_read = query_users \
                .join( query_questions, query_users.tag  == query_questions.tags ) \
                .withColumnRenamed("title","questions") \
                .select("userid","questions")



 
def write_to_cassandra_usertoquestion(df):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="stackoverflow", table="usertoquestion")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()
                


df_read.printSchema()

write_to_cassandra_usertoquestion(df_read)
