from sys import api_version
from kafka import KafkaConsumer 
from json import loads
from json import dumps
from src.connection import s3_connection
from src.config import BUCKET_NAME
from time import localtime
from time import time

def consumer_to_s3():
    # topic name
    str_topic_name = 'ecommerce'

    # topic, broker list 
    consumer = KafkaConsumer( 
        str_topic_name, 
        bootstrap_servers=['localhost:9092'], 
        auto_offset_reset='earliest',       # latest, earliest
        enable_auto_commit=True, 
        group_id='my-group', 
        value_deserializer=lambda x: loads(x.decode('utf-8')), 
        consumer_timeout_ms=1000,
        api_version=(0,10,0)) 
        
    # consumer list를 가져온다 
    messageList = []
    print('[begin] get consumer list') 
    for message in consumer: 
        print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" 
        % ( 
            message.topic, message.partition, message.offset, message.key, message.value 
            )) 
        print('[end] get consumer list')
        messageList += message.value

    if not(messageList):
        print("message is null")
        return True

    # conn to s3
    s3 = s3_connection()
    
    # s3 key
    tm = localtime(time())
    s3_key = f'raw_data/{tm.tm_year}_{tm.tm_mon}_{tm.tm_mday}_{tm.tm_hour}_{tm.tm_min}_ecommerce_log.json'

    # put json object to s3
    s3.put_object(
        Bucket = BUCKET_NAME,
        Body = dumps(messageList, indent=4),
        Key = s3_key,
    )
    print(messageList)