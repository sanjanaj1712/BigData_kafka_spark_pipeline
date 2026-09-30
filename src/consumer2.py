from kafka import KafkaConsumer
import json
import csv
import os

KAFKA_BROKER_IP='192.168.191.142:9092'
TOPICS=['topic-net', 'topic-disk']
NET_CSV_FILE = 'net_data.csv'
DISK_CSV_FILE = 'disk_data.csv'

print("Starting Consumer 2 for Network and Disk data")
consumer=KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER_IP],
    auto_offset_reset='earliest',
    group_id='consumer-group-2-fresh',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def initialize_csv(file_path, headers):
    if not os.path.exists(file_path):
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

initialize_csv(NET_CSV_FILE, ['ts', 'server_id', 'net_in', 'net_out'])
initialize_csv(DISK_CSV_FILE, ['ts', 'server_id', 'disk_io'])

print("Waiting for messages")
try:
    for message in consumer:
        topic=message.topic
        data=message.value
        if topic=='topic-net':
            with open(NET_CSV_FILE, mode='a', newline='') as file:
                writer=csv.writer(file)
                writer.writerow([data['ts'], data['server_id'], data['net_in'], data['net_out']])
            print(f"Saved NET data for server {data['server_id']}")
        elif topic=='topic-disk':
            with open(DISK_CSV_FILE, mode='a', newline='') as file:
                writer=csv.writer(file)
                writer.writerow([data['ts'], data['server_id'], data['disk_io']])
            print(f"Saved DISK data for server {data['server_id']}")
except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()