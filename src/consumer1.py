from kafka import KafkaConsumer
import json
import csv
import os

KAFKA_BROKER_IP='192.168.191.142:9092'
TOPICS=['topic-cpu','topic-mem']
CPU_CSV_FILE='cpu_data.csv'
MEM_CSV_FILE='mem_data.csv'

print("Starting Consumer 1 for CPU and Memory data...")
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER_IP],
    auto_offset_reset='earliest',
    group_id='consumer-group-1-fresh',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def initialize_csv(file_path, headers):
    if not os.path.exists(file_path):
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

initialize_csv(CPU_CSV_FILE, ['ts', 'server_id', 'cpu_pct'])
initialize_csv(MEM_CSV_FILE, ['ts', 'server_id', 'mem_pct'])

print("Waiting for messages...")
try:
    for message in consumer:
        topic = message.topic
        data = message.value
        if topic == 'topic-cpu':
            with open(CPU_CSV_FILE, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([data['ts'], data['server_id'], data['cpu_pct']])
            print(f"Saved CPU data for server {data['server_id']}")
        elif topic == 'topic-mem':
            with open(MEM_CSV_FILE, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([data['ts'], data['server_id'], data['mem_pct']])
            print(f"Saved MEM data for server {data['server_id']}")
except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()