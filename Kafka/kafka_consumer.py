from kafka import KafkaConsumer 
from json import loads 
from conn_hbase import connect_to_hbase
from conn_hbase import insert_row
import time

start_time = time.time()
# topic, broker list 
consumer = KafkaConsumer( 
    'test', 
    bootstrap_servers=['localhost:9093'], 
    auto_offset_reset='none',       # latest, earliest, none
    enable_auto_commit=True, 
    group_id='my-group', 
    value_deserializer=lambda x: loads(x.decode('utf-8')), 
    consumer_timeout_ms=1000 ) 
    
# consumer list를 가져온다 
messageList = []
print('[begin] get consumer list') 
for message in consumer: 
    print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" 
    % ( 
        message.topic, message.partition, message.offset, message.key, message.value 
        )) 
    print('[end] get consumer list')
    for log in message.value:
        log['event_date'] = log['event_time'][:10]
    messageList += message.value


hostname = 'localhost'
port=16010
namespace = 'ecommerce'
table_name = 'logs'
batch_size = 1000
row_count = 0
start_time = time.time()

conn, table, batch = connect_to_hbase(
    hostname=hostname,
    port=port,
    namespace=namespace,
    table_name=table_name,
    batch_size=1000
    )
print("Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size))

import struct
def float_to_bin(value):  # For testing.
    """ Convert float to 64-bit binary string. """
    [d] = struct.unpack(">Q", struct.pack(">d", value))
    return '{:064b}'.format(d)

for msg in messageList:
    row_count += 1
    if msg['category_code'] == None:
        msg['category_code'] = ''
    if msg['brand'] == None:
        msg['brand'] = ''

    row = [
        (msg['event_date'] + '_' + str(msg['user_id'])).encode('ascii'),   # Rowkey -> '2019-11-20_558420307'
        str(msg['user_id']).encode('ascii'),         # u:u_id -> 558420307
        (msg['user_session']).encode('ascii'),    # u:u_se -> '2761af5c-303c-4a1d-b9fc-46c4e05b1f6c'
        (msg['event_date']).encode('ascii'),      # t:ed -> '2019-11-20'
        (msg['event_time']).encode('ascii'),      # t:et -> '2019-11-20 06:33:23 UTC'
        (msg['event_type']).encode('ascii'),      # pd:et -> 'view
        str(msg['product_id']).encode('ascii'),      # pd:pi -> 13300069
        str(msg['category_id']).encode('ascii'),     # pd:ci -> 2053013557166998015
        (msg['category_code']).encode('ascii'),   # pd:cc -> None or 'appliances.environment.vacuum'
        (msg['brand']).encode('ascii'),           # pd:br -> None or 'xiaomi'
        str(msg['price']).encode('ascii')           # pd.pr -> 164.71
    ]
    insert_row(batch=batch, row=row)        # batch_size만큼 가득차면 send()함

batch.send()        # 남은 데이터 send()
conn.close()
duration = time.time() - start_time
print("Done. row count: %i, duration: %.3f s" % (row_count, duration))