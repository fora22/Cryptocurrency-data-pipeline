from sys import api_version
from kafka import KafkaProducer
from json import dumps
import time
import pandas as pd
import json
import os, shutil
from random import randint

def is_zero_dir(fpath):  
    return not(os.path.isdir(fpath) and os.path.getsize(fpath) > 0)

def select_file():
    if is_zero_dir('./rawdata/Nov_split/'):
        return None
    num = -1
    file_path = ''
    while True:
        num = randint(0, 99)
        file_path = f'./rawdata/Nov_split/{num}.parquet'
        if os.path.isdir(file_path):
            break
    return file_path

def on_send_success(record_metadata):
    # 보낸데이터의 매타데이터를 출력한다
    print("record_metadata:", record_metadata)

def producer_to_topic():
    file_path = select_file()
    sended_path = './rawdata/sended_dir/Nov/'
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(e)
        print('dir is not exist!!')

        bootstrap_servers = ["localhost:9092"]

        # 카프카 공급자 생성
        producer = KafkaProducer( acks=0,
                                bootstrap_servers=bootstrap_servers,
                                compression_type='gzip',
                                key_serializer=None,
                                value_serializer=lambda x: dumps(x).encode('utf-8'),
                                api_version=(0, 10, 1)
                                )

        # 카프카 토픽
        str_topic_name = 'ecommerce'

        # 카프카 공급자 토픽에 데이터를 보낸다
        start = time.time()
        data = df
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
            print(part_of_data)
            data = data.drop(temp.index)
            
        print("elapsed :", time.time() - start)

        shutil.move(file_path, sended_path)     # file move