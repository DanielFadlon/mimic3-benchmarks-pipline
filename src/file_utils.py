import json
from typing import Dict

def load_json(file_path: str) -> Dict:
    # Load the JSON content into a Python dictionary
    with open(file_path, 'r') as file:
        json_object = json.load(file)

    return json_object
