import csv
from kafka import KafkaProducer
import json
import time 
import sys 

KAFKA_BROKER_IP='192.168.191.142:9092'
CSV_FILE_PATH='Raw data/dataset.csv'

TOPIC_CPU='topic-cpu'
TOPIC_MEM='topic-mem'
TOPIC_NET='topic-net'
TOPIC_DISK='topic-disk'

print("Starting kafka Producer")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_IP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sent_count=0
error_count=0

with open(CSV_FILE_PATH, mode='r') as file:
    csv_reader = csv.DictReader(file)
    for index, row in enumerate(csv_reader):
        record_number=index+1
       
        
        try:
            cpu_data = {'ts': row['ts'], 'server_id': row['server_id'], 'cpu_pct': float(row['cpu_pct'])}
            mem_data = {'ts': row['ts'], 'server_id': row['server_id'], 'mem_pct': float(row['mem_pct'])}
           
            net_data = {'ts': row['ts'], 'server_id': row['server_id'],
                        'net_in': float(row['net_in']), 'net_out': float(row['net_out'])}
            disk_data = {'ts': row['ts'], 'server_id': row['server_id'], 'disk_io': float(row['disk_io'])}

            
            producer.send(TOPIC_CPU, value=cpu_data)
            producer.send(TOPIC_MEM, value=mem_data)
            producer.send(TOPIC_NET, value=net_data)
            producer.send(TOPIC_DISK, value=disk_data)
           
            sent_count = record_number
            sys.stdout.write(f"\rSent record #{record_number}...")
            sys.stdout.flush()

        except ValueError as e:
            error_count+=1
            print(f"\n[ERROR] Row {record_number} skipped due to non-numeric data in CSV: {e}", file=sys.stderr)
            print(f"Row data: {row}", file=sys.stderr)
         

print("\nProducer flushing buffered messages...")
producer.flush(timeout=10) 

print(f"--- Production Complete ---")
print(f"Total records processed: {sent_count + error_count}")
print(f"Total records sent to Kafka: {sent_count}")
print(f"Total records skipped due to data error: {error_count}")

producer.close()


