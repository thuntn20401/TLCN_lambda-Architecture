# TLCN-lambda-Architecture
# Lambda Architecture
Lambda Architecture implementation using Kafka for stream ingestion, Spark for batch and stream processing, HDFS and Cassandra for storage and querying, Flask for live visualization, and Docker for deployment.

# Run Batch
docker exec spark-master /spark/bin/spark-submit --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" /batch/batch_job.py

# Run Stream 
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_questions.py
<br/>
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_tags.py
<br/>
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_users.py
