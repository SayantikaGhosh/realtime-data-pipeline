from confluent_kafka import Producer
from faker import Faker
import json
import time
import random
import socket
import uuid  # To generate unique event IDs

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

wait_for_kafka("kafka", 9092)

conf = {
    'bootstrap.servers': 'kafka:9092'
}
p = Producer(conf)

# Callback to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")

# Product catalog (unchanged)
products = [
    {"product_id": "P001", "name": "Laptop", "category": "Electronics", "brand": "BrandA", "price": 1200.0},
    {"product_id": "P002", "name": "Headphones", "category": "Electronics", "brand": "BrandB", "price": 150.0},
    {"product_id": "P003", "name": "Shoes", "category": "Fashion", "brand": "BrandC", "price": 80.0},
    {"product_id": "P004", "name": "Watch", "category": "Accessories", "brand": "BrandD", "price": 200.0},
    {"product_id": "P005", "name": "Book", "category": "Education", "brand": "BrandE", "price": 25.0},
    {"product_id": "P006", "name": "Smartphone", "category": "Electronics", "brand": "BrandF", "price": 900.0},
    {"product_id": "P007", "name": "Tablet", "category": "Electronics", "brand": "BrandG", "price": 450.0},
    {"product_id": "P008", "name": "Backpack", "category": "Fashion", "brand": "BrandH", "price": 60.0},
    {"product_id": "P009", "name": "Sunglasses", "category": "Accessories", "brand": "BrandI", "price": 120.0},
    {"product_id": "P010", "name": "Desk Lamp", "category": "Home", "brand": "BrandJ", "price": 35.0},
    {"product_id": "P011", "name": "Gaming Console", "category": "Electronics", "brand": "BrandK", "price": 500.0},
    {"product_id": "P012", "name": "Coffee Maker", "category": "Home Appliances", "brand": "BrandL", "price": 75.0},
    {"product_id": "P013", "name": "T-shirt", "category": "Fashion", "brand": "BrandM", "price": 20.0},
    {"product_id": "P014", "name": "Jeans", "category": "Fashion", "brand": "BrandN", "price": 50.0},
    {"product_id": "P015", "name": "Bluetooth Speaker", "category": "Electronics", "brand": "BrandO", "price": 100.0},
    {"product_id": "P016", "name": "Microwave", "category": "Home Appliances", "brand": "BrandP", "price": 120.0},
    {"product_id": "P017", "name": "Water Bottle", "category": "Accessories", "brand": "BrandQ", "price": 15.0},
    {"product_id": "P018", "name": "Fitness Tracker", "category": "Electronics", "brand": "BrandR", "price": 130.0},
    {"product_id": "P019", "name": "Notebook", "category": "Education", "brand": "BrandS", "price": 5.0},
    {"product_id": "P020", "name": "Chair", "category": "Home", "brand": "BrandT", "price": 80.0}
]

# ✅ Pre-generate a fixed pool of users so they repeat across events
users = [{"user_id": str(uuid.uuid4()), "user_name": fake.name()} for _ in range(100)]

def generate_fake_event():
    product = random.choice(products)
    user = random.choice(users)  # pick from fixed pool instead of generating new each time
    return {
        "event_id": str(uuid.uuid4()),              # Unique event ID
        "user_id": user["user_id"],                 # Reused user IDs
        "user_name": user["user_name"],             # Same usernames tied to IDs
        "event_type": random.choice(["click", "purchase"]),  
        "product_id": product["product_id"],       
        "product_name": product["name"],           
        "category": product["category"],           
        "brand": product["brand"],                 
        "price": round(product["price"] * random.uniform(0.9, 1.1), 2),  
        "timestamp": fake.iso8601()                
    }

NUM_EVENTS = 200

for _ in range(NUM_EVENTS):
    event = generate_fake_event()
    message = json.dumps(event)
    p.produce('test-topic', message.encode('utf-8'), callback=delivery_report)
    p.poll(1)
    time.sleep(0.2)

p.flush()
