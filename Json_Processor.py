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

    def _merge_list_objects(self, data_list):
        """Merge list objects with different structures into a single DataFrame."""
        all_keys = set().union(*(d.keys() for d in data_list if isinstance(d, dict)))
        return pd.DataFrame([{key: obj.get(key, None) for key in all_keys} for obj in data_list])

    def _flatten_json(self, data, parent_name="root"):
        """Flattens JSON while keeping column names unchanged and structuring child tables."""
        flattened = {}
        child_tables = {}

        def _flatten(obj, current_parent):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _flatten(v, current_parent + self.separator + k)  # Update parent-child naming
            elif isinstance(obj, list):
                table_name = current_parent  # Use parent-child format
                child_tables[table_name] = self._merge_list_objects(obj)  # Convert list to DataFrame
            else:
                flattened[current_parent.split(self.separator)[-1]] = obj  # Keep column names unchanged

        for key, value in data.items():
            _flatten(value, parent_name + self.separator + key if isinstance(value, (list, dict)) else parent_name)

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
