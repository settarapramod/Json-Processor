import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json
import os

KAFKA_BROKER = 'localhost:9092'  # Update if needed
TOPIC = 'test_topic'  # Ensure it matches producer topic
OUTPUT_FILE = 'kafka_messages.txt'  # File to store messages

class WriteToFile(beam.DoFn):
    def __init__(self, file_path):
        self.file_path = file_path

    def setup(self):
        self.file = open(self.file_path, 'a')  # Append mode

    def process(self, element):
        key, value = element
        message = json.loads(value.decode('utf-8'))
        self.file.write(json.dumps(message) + '\n')
        self.file.flush()  # Ensure immediate writing
        yield message  # Pass along data if needed

    def teardown(self):
        self.file.close()

def run():
    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_BROKER,
                    "group.id": "beam-consumer-group",  # Ensures messages are assigned correctly
                    "auto.offset.reset": "earliest"  # Reads from beginning if new consumer
                },
                topics=[TOPIC]
            )
            | "Write to File" >> beam.ParDo(WriteToFile(OUTPUT_FILE))
        )

if __name__ == '__main__':
    run()
