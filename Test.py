import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(streaming=True)

with beam.Pipeline(options=options) as p:
    (
        p 
        | "Read Kafka" >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": "localhost:9092",
                "group.id": "my_consumer_group",
                "auto.offset.reset": "earliest",  # Read all messages from the beginning
            },
            topics=["my_topic"]
        )
        | "Print Messages" >> beam.Map(lambda msg: print(msg))
    )
