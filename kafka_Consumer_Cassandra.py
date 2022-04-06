from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
from kafka import KafkaConsumer 
from json import loads 

# topic, broker list 
consumer = KafkaConsumer( 
    'test', 
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest',       # latest
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


cluster = Cluster(['localhost'])
session = cluster.connect('mykeyspace')
session.row_factory = tuple_factory

for msg in messageList:
    sqlQuery = f'''
    INSERT INTO mykeyspace.eCommerce_log 
    (
        user_id, 
        user_session, 
        event_date, 
        event_time, 
        event_type, 
        product_id, 
        category_id, 
        category_code, 
        brand, 
        price
    )
    VALUES
    (
        {msg['user_id']},
        '{msg['user_session']}',
        '{msg['event_date']}',
        '{msg['event_time']}',
        '{msg['event_type']}',
        {msg['product_id']},
        {msg['category_id']},
        '{msg['category_code']}',
        '{msg['brand']}',
        {msg['price']}
    )
    ;
    '''
    session.execute(sqlQuery)

print(messageList)