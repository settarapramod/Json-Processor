import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

# Kafka configurations
BOOTSTRAP_SERVERS = 'localhost:9092'  # Update this
TOPIC = 'your_topic'  # Update this
GROUP_ID = 'beam_consumer_group'  # Change to any unique group ID

class PrintMessages(beam.DoFn):
    def process(self, element):
        key, value = element
        print(f"Key: {key}, Value: {value.decode('utf-8')}")  # Assuming value is bytes
        yield value.decode('utf-8')

def run():
    pipeline_options = PipelineOptions(streaming=True)  # Enable streaming mode
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | 'ReadFromKafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': BOOTSTRAP_SERVERS,
                    'group.id': GROUP_ID,  # Consumer group ID
                    'auto.offset.reset': 'earliest'  # Start from the beginning
                },
                topics=[TOPIC]
            )
            | 'PrintMessages' >> beam.ParDo(PrintMessages())
        )

if __name__ == '__main__':
    run()
