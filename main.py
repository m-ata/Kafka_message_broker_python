# from kafka import KafkaProducer
# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka.errors import TopicAlreadyExistsError
# import time
# import json

# admin_client = KafkaAdminClient(
#     bootstrap_servers='192.168.2.103:9092',
#     client_id='test-admin-tracker'
# )

# topic_list = [NewTopic(name="tracker", num_partitions=3, replication_factor=1)]

# try:
#     admin_client.create_topics(new_topics=topic_list, validate_only=False)
#     print("[Admin] Topic created: tracker")
# except TopicAlreadyExistsError:
#     print("[Admin] Topic already exists: tracker")

# def init():
#     producer = KafkaProducer(
#         bootstrap_servers='192.168.2.103:9092',
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )

#     for i in range(50):
#         # partition = round(i % 3)  # 0, 1, 2
#         data = {
#             'user_id': i,
#             'event': 'location-track',
#             'timestamp': time.time(),
#             # "partition": partition
#         }
#         producer.send('tracker', value=data, )
#         # producer.send('tracker', value=data, partition=partition)
#         print(f"[Producer] Sent to partition , {data}")
#         time.sleep(1)


#     producer.flush()
#     producer.close()

# def main():
#     init()

# if __name__ == "__main__":
#     main()


from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import json

admin_client = KafkaAdminClient(
    bootstrap_servers='192.168.2.103:9092',
    client_id='test-admin-tracker'
)

topic_name = "tracker"
num_partitions = 3

try:
    admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=1)])
    print(f"[Admin] Topic created: {topic_name}")
except TopicAlreadyExistsError:
    print(f"[Admin] Topic already exists: {topic_name}")

producer = KafkaProducer(
    bootstrap_servers='192.168.2.103:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def wait_for_partitions():
    print("[Info] Waiting for topic metadata...")
    while True:
        partitions = producer.partitions_for(topic_name)
        
        print(f"[Debug] Available partitions for topic '{topic_name}': {partitions}")
        
        if partitions and len(partitions) == num_partitions:
            print(f"[Info] Partitions for topic '{topic_name}': {partitions}")
            return partitions
        else:
            print("[Info] Still waiting for partitions...")
        time.sleep(1)

partitions = wait_for_partitions()

try:
    while True:
        partition = int(input(f"Enter the partition (0-{num_partitions - 1}): "))

        if partition not in partitions:
            print(f"[Error] Invalid partition number. Valid partitions are {partitions}.")
        else:
            data = {
                'user_id': partition,
                'event': 'location-track',
                'timestamp': time.time(),
                "partition": partition,
            }
            producer.send(topic_name, value=data, partition=partition)
            print(f"[Producer] Sent to partition {partition}: {data}")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[Producer] Exiting...")

finally:
    producer.flush()
    producer.close()
