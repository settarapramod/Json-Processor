from kafka import KafkaProducer
import json
import time

KAFKA_BROKER = 'localhost:9092'  # Change if using a different Kafka setup
TOPIC = 'test_topic'  # Change this to your topic name

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_messages():
    for i in range(10):  # Sending 10 messages
        msg = {'id': i, 'message': f'Hello Kafka {i}'}
        producer.send(TOPIC, value=msg)
        print(f'Produced: {msg}')
        time.sleep(1)  # Simulating delay

    producer.flush()

if __name__ == '__main__':
    produce_messages()
