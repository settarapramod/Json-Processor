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
