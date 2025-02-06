import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

# Kafka Consumer Configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092",  # Replace with your Kafka broker address
    "group.id": "beam_consumer",
    "auto.offset.reset": "earliest",  # Ensure it reads from the beginning
    "enable.auto.commit": "true"  # Ensure offset committing
}

options = PipelineOptions(streaming=True)

with beam.Pipeline(options=options) as p:
    (
        p 
        | "Read from Kafka" >> ReadFromKafka(
            consumer_config=kafka_config,
            topics=["my_topic"]
        )
        | "Decode Messages" >> beam.Map(lambda msg: (msg[0], msg[1].decode('utf-8')) if msg[1] else None)
        | "Print Messages" >> beam.Map(print)
    )
