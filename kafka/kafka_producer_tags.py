from kafka import KafkaProducer
import pandas as pd
import time
import json

df = pd.read_csv("data/tags_api.csv")

records = df.values.tolist()


producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))



while True:
    index = 0
    for record in records:
        record = tuple(record)
        to_send = {
            'tag':record[4],
            'count':record[3],
   
           
        }
        print(to_send)
        producer.send('tags-data', value=to_send)
        index += 1
        if (index % 100) == 0:
            time.sleep(10)

