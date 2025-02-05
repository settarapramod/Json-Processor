import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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
        messages = (
            pipeline
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS},
                topics=[KAFKA_TOPIC],
                # Optional: Set start_offset to 'earliest' to read from the beginning
                # start_offset='earliest'
            )
            | 'ExtractValue' >> beam.Map(lambda message: message[1])  # Extract value if message is (key, value)
            | 'Decode' >> beam.Map(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)  # Decode if needed
        )

        windowed_messages = (
            messages
            | 'WindowIntoFixedWindows' >> beam.WindowInto(
                beam.window.FixedWindows(size=5),  # 5-second windows (adjust as needed)
                trigger=beam.trigger.AfterWatermark(),
                accumulation_mode=beam.transforms.window.AccumulationMode.ACCUMULATING
            )
        )

        grouped_messages = (
            windowed_messages | 'GroupByKey' >> beam.GroupByKey()
        )

        processed_messages = (
            grouped_messages | 'ProcessGroupedMessages' >> beam.ParDo(ProcessGroupedData())
        )

        (
            processed_messages
            | 'Reshuffle' >> beam.Reshuffle() # Remove Window information
            | 'WriteToFile' >> beam.io.WriteToText(
                OUTPUT_FILE,
                file_name_suffix='.txt',
                append_trailing_newlines=True,
                coder=beam.coders.StrUtf8Coder()
            )
        )

        # Log messages (optional) - Can be useful for debugging
        # messages | 'LogMessages' >> beam.Map(lambda message: logging.info(f"Received message: {message}"))


class ProcessGroupedData(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, values = element
        count = len(list(values))  # Example: Count messages for each key
        output = f"Window: {window}, Key: {key}, Count: {count}"
        logging.info(output)
        yield output

if __name__ == '__main__':
    logging.info(f"Starting Kafka to File pipeline at {datetime.datetime.now()}")
    run()
    logging.info(f"Kafka to File pipeline finished at {datetime.datetime.now()}")
