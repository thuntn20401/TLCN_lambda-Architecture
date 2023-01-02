
# userprofile
def write_to_cassandra_userprofile(users_df):
    users_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="stackoverflow", table="userprofile")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()

#  toptags 
def write_to_cassandra_toptags(users_df):
    users_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(keyspace="stackoverflow", table="toptags")\
        .option("confirm.truncate", "true") \
        .mode("overwrite") \
        .save()


