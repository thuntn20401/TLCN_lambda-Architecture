# TLCN-lambda-Architecture
Đề tài: Tìm hiểu kiến trúc lambda cho xử lý dữ liệu và ứng dụng
Sinh viên thực hiện: 
Lê Thị Thanh Phương 19133046
Nguyễn Thị Nhả Thư  19133054

# Lambda Architecture
![image](https://user-images.githubusercontent.com/92160581/210227004-c3c2e123-7469-4c16-95a1-8a554860237c.png)

Lambda Architecture implementation using Kafka for stream ingestion, Spark for batch and stream processing, HDFS and Cassandra for storage and querying, Flask for live visualization, and Docker for deployment.

# Run Batch
docker exec spark-master /spark/bin/spark-submit --packages "com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" /batch/batch_job.py

# Run Stream 
## Create topic in Kafka
docker exec kafka kafka-topics --create --topic questions-data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
<br/>
docker exec kafka kafka-topics --create --topic tags-data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
<br/>
docker exec kafka kafka-topics --create --topic users-data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
<br/>
## Run Kafka Producer
python3 kafka_producer_tags.py 
<br/>
python3 kafka_producer_questions.py  
python3 kafka_producer_users.py

## Run Spark Streaming
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_questions.py
<br/>
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_tags.py
<br/>
docker exec spark-master /spark/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" stream/stream_users.py

# Run app
python3 app.py
