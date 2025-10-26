import time
from typing import Dict, List

import yaml


def get_top_level_config() -> Dict:
    with open('src/config/config.yml', 'r') as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)

    return yaml_object


def get_all_ids_from_config() -> List[str]:
    config = get_top_level_config()

    return list(config['dashboards'].keys())


def get_config_for_id(id: str) -> Dict:
    top_level_config = get_top_level_config()

    config = top_level_config['dashboards'][id]
    config['dashboard_id'] = id

    timestamp = int(time.time())
    config['database_file'] = f'box_office_db_{id}_{timestamp}.duckdb'

    bucket_config = top_level_config['bucket']
    config['bucket'] = bucket_config['bucket']

    var_names = [
        's3_read_access_key_id_var_name',
        's3_read_secret_access_key_var_name',
        's3_write_access_key_id_var_name',
        's3_write_secret_access_key_var_name',
    ]
    for var_name in var_names:
        config[var_name] = bucket_config.get(var_name, None)

    return config
