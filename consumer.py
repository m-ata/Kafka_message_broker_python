from kafka import KafkaConsumer
import json
import pprint

consumer = KafkaConsumer(
    'tracker',  
    bootstrap_servers='192.168.2.103:9092',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) 
)
# print(consumer.partitions_for_topic('tracker'))
pprint.pprint(consumer)


print("[Consumer] Listening for messages...\n")
for message in consumer:
    print(f"[Consumer] Received: {message.value}")
