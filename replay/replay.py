import json
import os
import time
import sys
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "orders-stream"
SCALE_FACTOR = float(os.getenv("SCALE_FACTOR", "86400"))
INPUT_FILE = "orders_grouped.jsonl"

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def ensure_topic(bootstrap_servers, topic_name):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    # Wait for broker to be available
    while True:
        try:
            metadata = admin_client.list_topics(timeout=5)
            if metadata:
                break
        except Exception:
            pass
        print("Waiting for Kafka...")
        time.sleep(2)

    topics = admin_client.list_topics().topics
    if topic_name not in topics:
        print(f"Creating topic {topic_name}...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        time.sleep(2)
    else:
        print(f"Topic {topic_name} already exists.")

def main():
    print(f"Starting replay. Scale factor: {SCALE_FACTOR}")
    ensure_topic(KAFKA_BOOTSTRAP_SERVERS, TOPIC)
    
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    previous_time = None
    
    try:
        with open(INPUT_FILE, "r") as f:
            for line in f:
                record = json.loads(line)
                current_time = datetime.fromisoformat(record["order_purchase_timestamp"])
                
                if previous_time is not None:
                    delta_seconds = (current_time - previous_time).total_seconds()
                    # It's possible for delta to be negative if data is slightly out of order, 
                    # though orders_grouped.jsonl should be sorted.
                    if delta_seconds > 0:
                        sleep_time = delta_seconds / SCALE_FACTOR
                        # Clip sleep time between 0.02s and 2s
                        sleep_time = max(0.02, min(sleep_time, 2.0))
                        time.sleep(sleep_time)
                
                producer.produce(TOPIC, value=json.dumps(record), callback=delivery_report)
                producer.poll(0)
                previous_time = current_time
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Please ensure it's mounted correctly.")
        sys.exit(1)
        
    producer.flush()
    print("Replay finished.")

if __name__ == "__main__":
    main()
