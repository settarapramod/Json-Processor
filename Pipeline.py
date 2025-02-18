import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json
import pyodbc

# Kafka Configuration
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic": "your_topic",
    "group_id": "beam_consumer"
}

# Database Configuration
DB_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=user;PWD=password"

# Flatten Function
def flatten_message(msg):
    """Flattens JSON message into a list of key-value dictionaries."""
    try:
        data = json.loads(msg)
        flattened_data = []
        
        def flatten_dict(d, parent_key=""):
            for k, v in d.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                if isinstance(v, dict):
                    flatten_dict(v, new_key)
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        flatten_dict({f"{k}_{i}": item}, parent_key)
                else:
                    flattened_data.append({new_key: v})

        flatten_dict(data)
        return flattened_data
    except Exception as e:
        print(f"Error flattening message: {e}")
        return []

# Database Write Function
def write_to_db(records, table_name):
    """Writes records to SQL Server."""
    try:
        conn = pyodbc.connect(DB_CONN_STR)
        cursor = conn.cursor()
        for record in records:
            columns = ", ".join(record.keys())
            values = ", ".join(f"'{v}'" for v in record.values())
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB Write Error: {e}")

class WriteToDB(beam.DoFn):
    """Apache Beam DoFn to write messages to DB."""
    def __init__(self, table_name):
        self.table_name = table_name

    def process(self, record):
        write_to_db([record], self.table_name)
        yield record  # Pass data forward if needed

# Apache Beam Pipeline
def run():
    pipeline_options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=pipeline_options) as p:
        kafka_messages = (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
                    "group.id": KAFKA_CONFIG["group_id"]
                },
                topics=[KAFKA_CONFIG["topic"]],
                key_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
                value_deserializer="org.apache.kafka.common.serialization.StringDeserializer"
            )
            | "ExtractMessage" >> beam.Map(lambda kv: kv[1])  # Get message value
        )

        # Write raw message to DB in parallel
        _ = kafka_messages | "WriteRawToDB" >> beam.ParDo(WriteToDB("raw_messages_table"))

        # Flatten and process the message
        processed_data = (
            kafka_messages
            | "FlattenMessage" >> beam.FlatMap(flatten_message)
            | "WriteFlattenedToDB" >> beam.ParDo(WriteToDB("flattened_messages_table"))
        )

if __name__ == "__main__":
    run()
