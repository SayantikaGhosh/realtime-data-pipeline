from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['test-topic'])

print("Consumer started. Waiting for messages...")

message_count = 0
MAX_MESSAGES = 5  # ✅ Change this number if you want more/less messages

try:
    while message_count < MAX_MESSAGES:
        msg = consumer.poll(1.0)  # timeout in seconds
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(f"✅ Received message: {msg.value().decode('utf-8')} from topic {msg.topic()}")
            message_count += 1

except KeyboardInterrupt:
    print("❌ Consumer interrupted manually.")

finally:
    print("✅ Closing consumer.")
    consumer.close()
