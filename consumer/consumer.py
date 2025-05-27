from confluent_kafka import Consumer, KafkaException
import json
import time
import socket
import sys

def wait_for_kafka(host, port, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[INFO] Kafka is available at {host}:{port}", flush=True)
                return
        except OSError:
            print(f"[WAIT] Waiting for Kafka at {host}:{port}...", flush=True)
            time.sleep(2)
    print(f"[ERROR] Kafka not available at {host}:{port} after {timeout} seconds", flush=True)
    sys.exit(1)

# Step 1: Wait for Kafka to be ready
wait_for_kafka("kafka", 9092)

# Step 2: Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

print("[STARTED] Kafka consumer started. Waiting for messages...", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[ERROR] {msg.error()}", flush=True)
            continue
        data = json.loads(msg.value().decode('utf-8'))
        print(f"[RECEIVED] {data['event_type'].upper()} by {data['user_id']} | Product: {data['product']} | ${data['price']} | {data['timestamp']}", flush=True)

except KeyboardInterrupt:
    print("[STOP] Consumer interrupted.", flush=True)

finally:
    print("[CLOSE] Closing Kafka consumer.", flush=True)
    consumer.close()
