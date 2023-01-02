from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, current_timestamp
spark_session = SparkSession \
    .builder \
    .appName("Streamer") \
    .config("spark.cassandra.connection.host","cassandra:9042" ) \
    .config("spark.executor.instances","1")\
    .getOrCreate()


dataframe = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "users-data")  \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")

dataframe = dataframe.select(

    get_json_object(dataframe.value, '$.id').alias('id'),
    get_json_object(dataframe.value, '$.reputation').alias('reputation')
   
)


df = dataframe.select("id","reputation")

query = df.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("checkpointLocation", '/tmp/stream/checkpoint/3/') \
    .option("keyspace", "stackoverflow") \
    .option("table", "realtimereputations") \
    .outputMode("append")\
    .start()

query.awaitTermination()
