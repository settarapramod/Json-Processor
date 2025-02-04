import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json
import pyodbc
import logging

class KafkaToSQLServerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--kafka_bootstrap_servers", type=str, help="Kafka bootstrap servers")
        parser.add_argument("--kafka_topic", type=str, help="Kafka topic to consume from")
        parser.add_argument("--sql_server_conn_str", type=str, help="SQL Server connection string")
        parser.add_argument("--sql_table", type=str, help="SQL table to write data")

class InsertToSQL(beam.DoFn):
    def __init__(self, conn_str, table_name):
        self.conn_str = conn_str
        self.table_name = table_name

    def start_bundle(self):
        """Initialize database connection"""
        self.conn = pyodbc.connect(self.conn_str)
        self.cursor = self.conn.cursor()

    def process(self, element):
        """Insert messages into SQL Server"""
        try:
            key, value = element
            msg = json.loads(value.decode("utf-8"))  # Decode and parse JSON message
            
            # Assuming 'data' column exists; adjust as needed
            query = f"INSERT INTO {self.table_name} (message) VALUES (?)"
            self.cursor.execute(query, json.dumps(msg))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error inserting record: {e}")
        
    def finish_bundle(self):
        """Close the database connection"""
        self.cursor.close()
        self.conn.close()

def run():
    pipeline_options = PipelineOptions()
    kafka_options = pipeline_options.view_as(KafkaToSQLServerOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={"bootstrap.servers": kafka_options.kafka_bootstrap_servers},
                topics=[kafka_options.kafka_topic]
            )
            | "Write to SQL Server" >> beam.ParDo(InsertToSQL(kafka_options.sql_server_conn_str, kafka_options.sql_table))
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
