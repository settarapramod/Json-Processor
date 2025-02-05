import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json

KAFKA_BROKER = 'localhost:9092'  # Change if needed
TOPIC = 'test_topic'  # Ensure this matches the producer

class PrintKafkaMessages(beam.DoFn):
    def process(self, element):
        key, value = element
        message = json.loads(value.decode('utf-8'))
        print(f'Polled: {message}')
        yield message

def run():
    pipeline_options = PipelineOptions(streaming=True)  # Enable streaming mode

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Kafka" >> ReadFromKafka(consumer_config={"bootstrap.servers": KAFKA_BROKER}, topics=[TOPIC])
            | "Process Messages" >> beam.ParDo(PrintKafkaMessages())
        )

if __name__ == '__main__':
    run()
