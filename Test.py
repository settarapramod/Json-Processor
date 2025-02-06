import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
import json

class KafkaSource(beam.DoFn):
    """Custom DoFn to read messages from Kafka"""
    
    def __init__(self, topic, bootstrap_servers, group_id):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
    
    def setup(self):
        """Initialize Kafka Consumer"""
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

    def process(self, element):
        """Poll Kafka messages in parallel"""
        for msg in self.consumer:
            yield msg.value  # Extract the actual message content

def run():
    # Kafka details
    kafka_topic = "your_topic"
    kafka_servers = ["localhost:9092"]
    kafka_group = "beam_group"

    # Apache Beam pipeline
    options = PipelineOptions(streaming=True)  # Enable streaming mode
    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Create PCollection" >> beam.Create([None])  # Dummy input to trigger KafkaSource
            | "Read from Kafka" >> beam.ParDo(KafkaSource(kafka_topic, kafka_servers, kafka_group))
            | "Print Messages" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()
