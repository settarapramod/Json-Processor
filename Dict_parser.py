def get_nested_value(data, key):
    """Fetch a value from a nested dictionary by key."""
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for v in data.values():
            result = get_nested_value(v, key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = get_nested_value(item, key)
            if result is not None:
                return result
    return None  # Return None if key is not found

# Example usage:
nested_data = {
    "user": {
        "name": "John",
        "details": {
            "email": "john@example.com",
            "address": {
                "city": "New York",
                "zipcode": "10001"
            }
        }
    }
}

print(get_nested_value(nested_data, "city"))   # Output: New York
print(get_nested_value(nested_data, "email"))  # Output: john@example.com
print(get_nested_value(nested_data, "zipcode"))  # Output: 10001
print(get_nested_value(nested_data, "country"))  # Output: None (key not found)
