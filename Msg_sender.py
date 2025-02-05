import time
import json
import random
from confluent_kafka import Producer

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Change this to your Kafka broker address
TOPICS = ["topic1", "topic2"]  # List of topics to send messages to
INTERVAL = 5  # Time interval in seconds

# Kafka Producer Configuration
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
}

# Initialize Kafka Producer
producer = Producer(producer_config)

def delivery_report(err, msg):
    """ Callback for message delivery confirmation """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_message():
    """ Generate a sample JSON message """
    return {
        "timestamp": time.time(),
        "value": random.randint(1, 100),
        "status": random.choice(["success", "error", "pending"])
    }

def produce_messages():
    """ Publish messages to Kafka topics at regular intervals """
    while True:
        for topic in TOPICS:
            message = json.dumps(generate_message())
            producer.produce(topic, value=message, callback=delivery_report)
        producer.flush()  # Ensure messages are sent
        time.sleep(INTERVAL)

if __name__ == "__main__":
    print(f"Starting Kafka producer. Sending messages every {INTERVAL} seconds...")
    produce_messages()
