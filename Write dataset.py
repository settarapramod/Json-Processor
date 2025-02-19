import pandas as pd import pyodbc import logging

Configure logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(sql_connection_str): """Create and return a pyodbc database connection.""" return pyodbc.connect(sql_connection_str)

def get_table_columns(conn, table_name): """Fetch existing columns from the SQL table.""" query = f""" SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' """ cursor = conn.cursor() cursor.execute(query) return {row[0].lower() for row in cursor.fetchall()}

def get_dataframe_columns(df): """Fetch and normalize column names from the dataset.""" return {col.lower() for col in df.columns}

def compare_columns(table_columns, dataset_columns): """Compare dataset columns with table columns and log missing ones.""" missing_from_dataset = table_columns - dataset_columns missing_from_table = dataset_columns - table_columns

if missing_from_dataset:
    logging.warning(f"Missing columns in dataset: {missing_from_dataset}")
if missing_from_table:
    logging.warning(f"Missing columns in table: {missing_from_table}")

return missing_from_dataset, missing_from_table

def clean_and_prepare_dataframe(df, table_columns, missing_from_dataset): """Drop columns not in the table and add missing ones with NULL values.""" df = df[[col for col in df.columns if col.lower() in table_columns]] for col in missing_from_dataset: df[col] = None  # Fill missing columns with NULL values return df

def convert_data_types(df, conn, table_name): """Convert data types to match SQL Server compatibility.""" type_map = { 'int': int, 'bigint': int, 'float': float, 'double': float, 'decimal': float, 'numeric': float, 'varchar': str, 'nvarchar': str, 'char': str, 'text': str, 'datetime': pd.to_datetime, 'timestamp': pd.to_datetime }

query = f"""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
"""

cursor = conn.cursor()
cursor.execute(query)
sql_types = {row[0].lower(): row[1] for row in cursor.fetchall()}

for col in df.columns:
    sql_type = sql_types.get(col, 'varchar')
    if sql_type in type_map:
        df[col] = df[col].apply(lambda x: type_map[sql_type](x) if pd.notnull(x) else None)

return df

def insert_data(df, conn, table_name): """Insert data into the SQL table.""" cursor = conn.cursor() columns = ", ".join(df.columns) placeholders = ", ".join(['?' for _ in df.columns]) query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

for _, row in df.iterrows():
    cursor.execute(query, tuple(row))
conn.commit()
logging.info("Data inserted successfully.")

def main(sql_connection_str, df, table_name): """Main function to orchestrate the workflow.""" conn = get_db_connection(sql_connection_str) table_columns = get_table_columns(conn, table_name) dataset_columns = get_dataframe_columns(df) missing_from_dataset, missing_from_table = compare_columns(table_columns, dataset_columns) df = clean_and_prepare_dataframe(df, table_columns, missing_from_dataset) df = convert_data_types(df, conn, table_name) insert_data(df, conn, table_name) conn.close()

Example Usage

df = pd.read_csv('dataset.csv')  # Load your dataset

sql_connection_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=database;UID=username;PWD=password"

main(sql_connection_str, df, 'target_table')
