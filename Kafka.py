import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json
import pyodbc
import logging
import configparser

# Load configurations from a config file
config = configparser.ConfigParser()
config.read("config.ini")

class KafkaToSQLServerOptions(PipelineOptions):
    """Pipeline options for Apache Beam"""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--config_file", type=str, help="Path to the config file")

class InsertToSQL(beam.DoFn):
    """Class to insert Kafka messages into SQL Server in real-time"""
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
            
            # Insert message into the table (Assumes a column 'message' exists)
            query = f"INSERT INTO {self.table_name} (message) VALUES (?)"
            self.cursor.execute(query, json.dumps(msg))
            self.conn.commit()
            logging.info(f"Inserted message into {self.table_name}: {msg}")
        except Exception as e:
            logging.error(f"Error inserting record: {e}")

    def finish_bundle(self):
        """Close the database connection"""
        self.cursor.close()
        self.conn.close()

class JSONProcessor(beam.DoFn):
    """Class to flatten JSON and print it"""
    def process(self, element):
        key, value = element
        msg = json.loads(value.decode("utf-8"))  # Decode JSON message
        flat_msg = self.flatten_json(msg)  # Flatten JSON
        logging.info(f"Flattened JSON: {flat_msg}")
        yield flat_msg  # Yield flattened JSON for further processing if needed

    def flatten_json(self, data, parent_key="", sep="_"):
        """Recursively flattens a nested JSON object"""
        items = {}
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self.flatten_json(v, new_key, sep=sep))
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    items.update(self.flatten_json({f"{new_key}{sep}{i}": item}, "", sep=sep))
            else:
                items[new_key] = v
        return items

def run():
    """Main function to run the pipeline"""
    pipeline_options = PipelineOptions()
    kafka_options = pipeline_options.view_as(KafkaToSQLServerOptions)

    # Load config details
    kafka_bootstrap_servers = config["KAFKA"]["bootstrap_servers"]
    kafka_topic = config["KAFKA"]["topic"]
    sql_server_conn_str = config["SQL"]["connection_string"]
    sql_table = config["SQL"]["table"]

    with beam.Pipeline(options=pipeline_options) as p:
        messages = (
            p
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={"bootstrap.servers": kafka_bootstrap_servers},
                topics=[kafka_topic]
            )
        )

        # Write messages to SQL Server in parallel
        _ = messages | "Write to SQL Server" >> beam.ParDo(InsertToSQL(sql_server_conn_str, sql_table))

        # Process messages in parallel (Flatten JSON)
        _ = messages | "Process JSON" >> beam.ParDo(JSONProcessor())

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
