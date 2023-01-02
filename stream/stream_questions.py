from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql import functions as F
from pyspark.sql.functions import substring, length, col, expr
from pyspark.sql.functions import trim, col,udf,split,explode,desc,rank,regexp_replace
from pyspark.sql.functions import trim, col,udf,split,explode,desc,rank
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
    .option("subscribe", "questions-data") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr( "CAST(key AS STRING)", "CAST(value AS STRING)")

dataframe = dataframe.select(

    get_json_object(dataframe.value, '$.tags').alias('tags'),
    get_json_object(dataframe.value, '$.title').alias('title')
   
)

substr_to_remove = [",","'"]
regex = "|".join(substr_to_remove)
df = dataframe.withColumn("tag", F.regexp_replace("tags", regex, "")) \
                  .withColumn('tag', trim(col('tag')))  \
                  .withColumn("tag1",expr("substring(tag, 2, length(tag)-2)"))\
                  .withColumn('tag1',explode(split('tag1'," ")))


df_result = df.select('tag1','title') \
              .withColumnRenamed("tag1","tags")

query = df_result.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("checkpointLocation", '/tmp/stream/checkpoint/1/') \
    .option("keyspace", "stackoverflow") \
    .option("table", "realtimequestions") \
    .outputMode("append")\
    .start()

query.awaitTermination()
