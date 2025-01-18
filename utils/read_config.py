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

    duckdb_con.close()


def get_config_for_id(id: str) -> Dict:
    config = get_top_level_config()['dashboards'][id]

    load_override_tables(config)

    return config
