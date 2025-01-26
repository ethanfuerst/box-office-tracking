import json
from typing import Dict, List

import yaml

from utils.db_connection import DuckDBConnection


def get_top_level_config() -> Dict:
    with open('config/config.yml', 'r') as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)

    return yaml_object


def get_all_ids_from_config() -> List[str]:
    config = get_top_level_config()

    return list(config['dashboards'].keys())


def load_override_tables(config: Dict) -> None:
    movie_overrides = config.get('movie_multiplier_overrides', [])
    round_overrides = config.get('round_multiplier_overrides', [])
    exclusions = config.get('exclusions', [])

    duckdb_con = DuckDBConnection(config)

    duckdb_con.execute(
        '''
        CREATE OR REPLACE TABLE movie_multiplier_overrides (
            movie VARCHAR,
            multiplier DOUBLE
        );
        CREATE OR REPLACE TABLE round_multiplier_overrides (
            round INTEGER,
            multiplier DOUBLE
        );
        CREATE OR REPLACE TABLE draft_year_exclusions (
            movie VARCHAR
        );
    '''
    )

    for override in movie_overrides:
        duckdb_con.execute(
            'INSERT INTO movie_multiplier_overrides VALUES (?, ?)',
            (override['movie'], override['multiplier']),
        )

    for override in round_overrides:
        duckdb_con.execute(
            'INSERT INTO round_multiplier_overrides VALUES (?, ?)',
            (override['round'], override['multiplier']),
        )

    for exclusion in exclusions:
        duckdb_con.execute(
            'INSERT INTO draft_year_exclusions VALUES (?)',
            (exclusion,),
        )

    duckdb_con.close()


def get_config_for_id(id: str) -> Dict:
    top_level_config = get_top_level_config()

    config = top_level_config['dashboards'][id]
    config['exclusions'] = top_level_config['draft_years'][config['year']]['exclusions']

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

    load_override_tables(config)

    return config
