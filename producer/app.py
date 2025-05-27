from confluent_kafka import Producer
import time

# Kafka configuration - use the Docker internal network name for Kafka broker
conf = {
    'bootstrap.servers': 'kafka:9092'  # Kafka broker address inside Docker network
}

# Create a Kafka Producer instance
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback function called once message delivery is confirmed or failed."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} partition [{msg.partition()}]")

# Send 5 example messages to Kafka topic 'test-topic'
for i in range(5):
    message = f"Hello Kafka {i}"
    # Produce/send the message to 'test-topic' asynchronously
    producer.produce('test-topic', message.encode('utf-8'), callback=delivery_report)
    
    # Serve delivery reports (calls the callback)
    producer.poll(1)
    
    # Wait for 1 second before sending next message
    time.sleep(1)

# Wait until all messages are sent and delivery reports received
producer.flush()
