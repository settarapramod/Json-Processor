import pandas as pd
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class JSONProcessor:
    """Processes JSON messages by flattening them into structured datasets."""

    def __init__(self, separator="_"):
        """Initialize JSON Processor with a separator for child table names."""
        self.separator = separator

    def _flatten_json(self, data, parent_name="root"):
        """Flattens JSON while preserving column names and structuring child tables."""
        flattened = {}
        child_tables = {}

        def _flatten(obj, current_parent):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _flatten(v, current_parent)  # Keep column names unchanged
            elif isinstance(obj, list):
                table_name = f"{current_parent}{self.separator}{k}"  # Parent name + separator + key name
                child_tables[table_name] = pd.DataFrame(obj)  # Convert list to DataFrame
            else:
                flattened[k] = obj  # Keep column names as is

        _flatten(data, parent_name)
        root_df = pd.DataFrame([flattened]) if flattened else pd.DataFrame()
        return {parent_name: root_df, **child_tables}

    def process_json(self, json_msg):
        """Processes a JSON message into a flattened dataset."""
        try:
            data = json.loads(json_msg) if isinstance(json_msg, str) else json_msg
            return self._flatten_json(data)
        except Exception as e:
            logging.error(f"Error processing JSON: {e}")
            return None  # Return None to indicate failure
