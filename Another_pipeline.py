import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub  # If using Pub/Sub
from apache_beam.io.kafka import ReadFromKafka
import logging
import datetime

# Configure logging
LOG_FILE = "kafka_to_file.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'your_kafka_brokers'  # Replace with your Kafka brokers
KAFKA_TOPIC = 'your_kafka_topic'  # Replace with your Kafka topic

# Output file configuration
OUTPUT_FILE = 'kafka_messages.txt'

def run(argv=None):
    pipeline_options = PipelineOptions(argv)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Kafka
        messages = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS},
                topics=[KAFKA_TOPIC],
                # Optional: Set start_offset to 'earliest' to read from the beginning
                # start_offset='earliest'
            )
            # If Kafka message has key and value, extract the value:
            | 'ExtractValue' >> beam.Map(lambda message: message[1])  # message is a tuple (key, value)
            | 'Decode' >> beam.Map(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)  # Decode if necessary
        )


        # Write to file in real-time (using a windowing approach)
        (
            messages
            | 'WindowIntoGlobal' >> beam.WindowInto(beam.window.GlobalWindows()) # For real-time, keep in global window
            | 'WriteToFile' >> beam.io.WriteToText(
                OUTPUT_FILE,
                file_name_suffix='.txt',
                append_trailing_newlines=True,
                coder=beam.coders.StrUtf8Coder() # Explicitly set the coder
            )
        )

        # Log the messages (optional - for debugging/monitoring)
        messages | 'LogMessages' >> beam.Map(lambda message: logging.info(f"Received message: {message}"))


if __name__ == '__main__':
    logging.info(f"Starting Kafka to File pipeline at {datetime.datetime.now()}")
    run()
    logging.info(f"Kafka to File pipeline finished at {datetime.datetime.now()}")



