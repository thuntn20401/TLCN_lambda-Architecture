from kafka import KafkaProducer
import pandas as pd
import time
import json

df = pd.read_csv("data/questions_api.csv")

records = df.values.tolist()


producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))



while True:
    index = 0
    for record in records:
        record = tuple(record)
        to_send = {
            'tags':record[0],
            'title':record[14],
   
           
        }
        print(to_send)
        producer.send('questions-data', value=to_send)
        index += 1
        if (index % 100) == 0:
            time.sleep(10)
