from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, current_timestamp
from pyspark.sql.functions import  col,split,desc,rank
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
    .option("subscribe", "tags-data") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")

dataframe = dataframe.select(

    get_json_object(dataframe.value, '$.tag').alias('tag'),
    get_json_object(dataframe.value, '$.count').alias('count')
   
)


df = dataframe.withColumn("updatedon", current_timestamp()) \
              .withColumnRenamed("tag","tagname") 
              
df_result = df.select( 'updatedon' , 'tagname' ,'count' ) 
            
	

query = df_result.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("checkpointLocation", '/tmp/stream/checkpoint/2/') \
    .option("keyspace", "stackoverflow") \
    .option("table", "trendingtags") \
    .outputMode("append")\
    .start()
     

query.awaitTermination()
