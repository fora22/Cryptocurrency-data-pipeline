from dataclasses import replace
from email import message
from kafka import KafkaProducer
from json import dumps
import time
import pandas as pd
import json


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").master('local').getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '100')

df = spark\
    .read\
    .parquet('./rawdata/Nov_split/1.parquet')

# df.show(10)
# df.columns
# df.dtypes

bootstrap_servers = ["localhost:9093"]

# 카프카 공급자 생성
producer = KafkaProducer( acks=0,
                        bootstrap_servers=bootstrap_servers,
                        compression_type='gzip',
                        key_serializer=None,
                        value_serializer=lambda x: dumps(x).encode('utf-8')
                        )


def on_send_success(record_metadata):
    # 보낸데이터의 매타데이터를 출력한다
    print("record_metadata:", record_metadata)

# 카프카 토픽
str_topic_name = 'test'

# 카프카 공급자 토픽에 데이터를 보낸다
start = time.time()
# data = json.loads(df.toPandas().to_json(orient='records'))
data = df.toPandas()
sendSize = 1000
while not(data.empty):
    if len(data) >= sendSize:
        temp = data.sample(n=sendSize,replace=False, random_state=333)
    else:
        temp = data.sample(n=len(data), replace=False, random_state=333)
    part_of_data = json.loads(temp.to_json(orient='records'))
    producer.send(str_topic_name, value=part_of_data).add_callback(on_send_success)\
                                        .get(timeout=100) # blocking maximum timeout
    producer.flush()
    
    data = data.drop(temp.index)
    
print("elapsed :", time.time() - start)
