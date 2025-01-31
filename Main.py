def run_pipeline(json_messages, sql_config):
    """Runs Apache Beam pipeline to process JSON messages in parallel and insert into SQL Server."""
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read JSON Messages" >> beam.Create(json_messages)
            | "Process JSON" >> beam.ParDo(ProcessJsonDoFn())
            | "Write to SQL" >> beam.ParDo(WriteToSQLDoFn(sql_config))
        )

sql_config = {
    "root_table": {
        "server": "my_server",
        "database": "my_db",
        "user": "my_user",
        "password": "my_password",
    },
    "details_table": {
        "server": "my_server",
        "database": "my_db",
        "user": "my_user",
        "password": "my_password",
    },
    "hobbies_table": {
        "server": "my_server",
        "database": "my_db",
        "user": "my_user",
        "password": "my_password",
    },
}

json_messages = [
    '{"id": 1, "name": "John", "details": {"age": 30, "city": "NY"}, "hobbies": ["reading", "sports"]}',
    '{"id": 2, "name": "Jane", "details": {"age": 25, "city": "LA"}, "hobbies": ["music", "travel"]}'
]

run_pipeline(json_messages, sql_config)
