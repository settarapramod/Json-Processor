import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
import json
import pyodbc  # Use psycopg2 for PostgreSQL instead

# Custom Kafka Reader
class KafkaSource(beam.DoFn):
    """Reads messages from Kafka in parallel"""
    
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
        """Poll messages and yield for further processing"""
        for msg in self.consumer:
            yield msg.value  # Extract actual message content

# Custom Database Writer
class WriteToSQL(beam.DoFn):
    """Writes messages to SQL Server"""
    
    def __init__(self, conn_string, table_name):
        self.conn_string = conn_string
        self.table_name = table_name

    def setup(self):
        """Establish Database Connection"""
        self.conn = pyodbc.connect(self.conn_string)
        self.cursor = self.conn.cursor()

    def process(self, message):
        """Insert Kafka message into SQL table"""
        try:
            # Assuming message is a dictionary with keys matching table columns
            columns = ", ".join(message.keys())
            values = ", ".join(["?" for _ in message.values()])
            sql_query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
            
            self.cursor.execute(sql_query, tuple(message.values()))
            self.conn.commit()
            
            yield f"Inserted into {self.table_name}: {message}"
        except Exception as e:
            yield f"Error inserting: {str(e)}"

def run():
    # Kafka details
    kafka_topic = "your_topic"
    kafka_servers = ["localhost:9092"]
    kafka_group = "beam_group"

    # SQL Database details (Change accordingly)
    db_conn_string = "DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password"
    db_table_name = "your_table"

    # Apache Beam pipeline
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create PCollection" >> beam.Create([None])  # Trigger KafkaSource
            | "Read from Kafka" >> beam.ParDo(KafkaSource(kafka_topic, kafka_servers, kafka_group))
            | "Write to SQL" >> beam.ParDo(WriteToSQL(db_conn_string, db_table_name))
            | "Print Messages" >> beam.Map(print)  # Print for debugging
        )

if __name__ == "__main__":
    run()
