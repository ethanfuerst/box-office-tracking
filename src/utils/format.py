import json
from typing import Dict, List, Union


def remove_comments(obj: Union[Dict, List]) -> Union[Dict, List]:
    if isinstance(obj, dict):
        return {
            k: remove_comments(v)
            for k, v in obj.items()
            if not k.startswith('_comment')
        }
    elif isinstance(obj, list):
        return [remove_comments(item) for item in obj]
    else:
        return obj


def load_format_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        config = json.load(file)
    return remove_comments(config)
