from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, col,udf,split,explode,desc,rank
from pyspark.sql.functions import avg, current_timestamp, format_number
from pyspark.sql.window import Window

from batch_write import write_to_cassandra_userprofile,write_to_cassandra_toptags

HDFS_NAMENODE = 'hdfs://namenode:9000'
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("lambda arch batch job") \
        .getOrCreate()




#  userprofile - id, displayname, upvotes, downvotes, reputation
    df_users_read  = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/lambda-arch/data/Users.csv")\
            .withColumnRenamed( "Id", "id" ) \
            .withColumnRenamed( "DisplayName", "displayname" ) \
            .withColumnRenamed( "UpVotes", "upvotes" ) \
            .withColumnRenamed( "DownVotes", "downvotes" ) \
            .withColumnRenamed( "Reputation", "reputation" ) \
            .withColumn("upvotes",col('upvotes').cast(IntegerType()))\
            .withColumn("downvotes",col('downvotes').cast(IntegerType()))\
            .withColumn("reputation",col('reputation').cast(IntegerType()))\
            .select( 'id', 'displayname', 'upvotes', 'downvotes', 'reputation' )  

    write_to_cassandra_userprofile(df_users_read)
        
# toptags - userid, tags
   
    df_post1 =  spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/lambda-arch/data/Posts.csv")
    df_post2 =  spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/lambda-arch/data/Posts.csv")

    df_read_tagtop = df_post1 \
                .withColumnRenamed( "Id", "id1" ) \
                .withColumnRenamed( "OwnerUserId", "OwnerUserId1" ) \
                .withColumnRenamed( "Title", "Title1" ) \
                .withColumnRenamed( "Tags", "Tags1" ) \
                .withColumnRenamed("Score","Score1")  \
                .filter(col('AcceptedAnswerId').isNotNull()) \
                .join( df_post2, df_post1.AcceptedAnswerId  == df_post2.Id ) \
                .select('Id','OwnerUserId','Title1','Tags1','Score')
         
    df_result = df_read_tagtop \
            .filter(col('OwnerUserId').isNotNull()) \
            .withColumnRenamed("Id","postid") \
            .withColumnRenamed("OwnerUserId","userid")\
            .withColumnRenamed("Title1","title") \
            .withColumnRenamed("Tags1","tags") \
            .withColumnRenamed("Score","score") 
           



# df_result = df_result.withColumn("tag", split(col('tags') , "[<>]" )) 
    substr_to_remove = ["<",">"]
    regex = "|".join(substr_to_remove)
    df_usertag = df_result.withColumn("tag", F.regexp_replace("tags", regex, " ")) \
                  .withColumn('tag', trim(col('tag')))  \
                  .withColumn('tag',explode(split('tag',"  ")))


    df_top =   df_usertag.groupBy(["userid", "tag"]).count().orderBy( desc("count"))  
                


    column_list = ["userid"]
    window = Window.partitionBy(column_list).orderBy( desc("count"))
    df_select = df_top  \
             .withColumn("rank", rank().over(window)) \
             .filter(col('rank') <= 3) \
             .select('userid', 'tag')


    write_to_cassandra_toptags(df_select)

    print("_____________DONE____________")







