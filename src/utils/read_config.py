from typing import Dict

import yaml


def get_config_dict(config_path: str) -> Dict:
    with open(config_path, 'r') as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)

    return yaml_object
