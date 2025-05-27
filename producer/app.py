from confluent_kafka import Producer
from faker import Faker
import json
import time
import random
import socket

fake = Faker()

def wait_for_kafka(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[WAIT] Kafka is available at {host}:{port}")
                return True
        except OSError:
            print(f"[WAIT] Waiting for Kafka at {host}:{port}...")
            time.sleep(2)
    raise TimeoutError(f"Kafka not available at {host}:{port} after {timeout} seconds")

# Wait until Kafka is up before proceeding
wait_for_kafka("kafka", 9092)

conf = {
    'bootstrap.servers': 'kafka:9092'
}

p = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_fake_event():
    return {
        "user_id": fake.uuid4(),
        "event_type": random.choice(["click", "purchase"]),
        "product": fake.word(),
        "price": round(random.uniform(10.0, 500.0), 2),
        "timestamp": fake.iso8601()
    }

for _ in range(10):
    event = generate_fake_event()
    message = json.dumps(event)
    p.produce('test-topic', message.encode('utf-8'), callback=delivery_report)
    p.poll(1)
    time.sleep(1)

p.flush()
