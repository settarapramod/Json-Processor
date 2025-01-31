import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc

class ProcessJsonDoFn(beam.DoFn):
    """Apache Beam DoFn to process JSON messages."""
    
    def __init__(self):
        super().__init__()
        self.processor = JSONProcessor()

    def process(self, element):
        """Processes each JSON element and yields a dictionary of datasets."""
        datasets = self.processor.process_json(element)
        if datasets:
            yield datasets  # Output dict of {table_name -> DataFrame}

class WriteToSQLDoFn(beam.DoFn):
    """Apache Beam DoFn to insert processed data into SQL Server."""
    
    def __init__(self, config):
        super().__init__()
        self.config = config  # Table configuration dictionary

    def start_bundle(self):
        """Set up a database connection (optional for performance)."""
        self.connections = {}

    def insert_data(self, table_name, df):
        """Insert DataFrame into SQL table with schema handling."""
        conn_details = self.config.get(table_name)
        if not conn_details:
            logging.warning(f"Skipping table {table_name} as it is not in config.")
            return

        conn_str = f"DRIVER={{SQL Server}};SERVER={conn_details['server']};DATABASE={conn_details['database']};UID={conn_details['user']};PWD={conn_details['password']}"
        
        # Reuse connection if available
        if table_name not in self.connections:
            self.connections[table_name] = pyodbc.connect(conn_str)
        
        conn = self.connections[table_name]
        cursor = conn.cursor()
        
        # Fetch table columns
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        table_columns = {row.COLUMN_NAME for row in cursor.fetchall()}

        # Adjust DataFrame to match SQL table schema
        df = df.reindex(columns=table_columns, fill_value=None)

        # Insert rows
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
        
        try:
            cursor.executemany(insert_query, df.values.tolist())
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting into {table_name}: {e}")
        finally:
            cursor.close()

    def finish_bundle(self):
        """Close database connections."""
        for conn in self.connections.values():
            conn.close()
        self.connections.clear()

    def process(self, datasets):
        """Insert each dataset into its respective SQL table."""
        for table_name, df in datasets.items():
            self.insert_data(table_name, df)
