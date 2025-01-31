import pandas as pd
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class JSONProcessor:
    """Processes JSON messages by flattening them into structured datasets."""
    
    def __init__(self, separator='_'):
        """Initialize JSON Processor with a separator for nested keys."""
        self.separator = separator

    def _flatten_json(self, data, parent_key=''):
        """Recursively flattens a nested JSON object, handling lists as child tables."""
        flattened = {}
        child_tables = {}

        def _flatten(obj, key_prefix=''):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{key_prefix}{self.separator}{k}" if key_prefix else k
                    _flatten(v, new_key)
            elif isinstance(obj, list):
                table_name = f"{parent_key}{self.separator}{key_prefix}" if parent_key else key_prefix
                child_tables[table_name] = pd.DataFrame(obj)  # Convert list to DataFrame
            else:
                flattened[key_prefix] = obj

        _flatten(data)
        return flattened, child_tables

    def process_json(self, json_msg):
        """Processes JSON message into a flattened dataset."""
        try:
            data = json.loads(json_msg) if isinstance(json_msg, str) else json_msg
            root_table, child_tables = self._flatten_json(data)
            root_df = pd.DataFrame([root_table])  # Convert root JSON to DataFrame
            return {"root_table": root_df, **child_tables}
        except Exception as e:
            logging.error(f"Error processing JSON: {e}")
            return None  # Return None to indicate failure
