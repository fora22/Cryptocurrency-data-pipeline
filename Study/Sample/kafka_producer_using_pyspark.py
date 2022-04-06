from kafka import KafkaProducer
from json import dumps
import time
import pandas as pd
import json


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").master('local').getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '5')

df = spark\
    .read\
    .option("inferSchema", "true")\
    .option('header', 'true')\
    .csv('./rawdata/2019-Oct.csv')    

# df = spark\
#     .readStream\
#     .option("maxFilePerTrigger", 1)\
#     .format("csv")\
#     .option('header', 'true')\
#     .load('./rawdata/2019-N*.csv')   

# df.show(10)
# df.columns
# df.dtypes

bootstrap_servers = ["localhost:9092"]

# 카프카 공급자 생성
producer = KafkaProducer( acks=0,
                        bootstrap_servers=bootstrap_servers,
                        compression_type='gzip',
                        key_serializer=None,
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

def on_send_success(record_metadata):
    # 보낸데이터의 매타데이터를 출력한다
    print("record_metadata:", record_metadata)
    
# 카프카 토픽
str_topic_name = 'test'

# 카프카 공급자 토픽에 데이터를 보낸다
start = time.time()

from pyspark.sql.functions import monotonically_increasing_id 
temp_index = temp.select("*").withColumn("id", monotonically_increasing_id())

start = time.time()
for i in range(10):
    temp = df.sample(False, 0.00001, 333)
    part_of_data = json.loads(temp.toPandas().to_json())
    # data = {"time": time.time()}
    data = {'time': 'result' + str(i)}
    producer.send(str_topic_name, value=part_of_data).add_callback(on_send_success)\
                                         .get(timeout=100) # blocking maximum timeout
    producer.flush()
print("elapsed :", time.time() - start)

# temp_index.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")\
#     .write\
#     .format('kafka')\
#     .option("topic", 'test')\
#     .option("kafka.bootstrap.servers", "localhost:9092")\
#     .option("checkpointLocation", "./checkpoint") \
#     .save()
'''
for i in range(10):
    # data = {"time": time.time()}
    data = {'time': 'result' + str(i)}
    producer.send(str_topic_name, value=data).add_callback(on_send_success)\
                                         .get(timeout=100) # blocking maximum timeout
    producer.flush()
'''
print("elapsed :", time.time() - start)