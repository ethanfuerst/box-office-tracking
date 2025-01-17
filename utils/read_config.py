import json
from typing import Dict, List

import yaml

from utils.db_connection import DuckDBConnection


def read_config() -> Dict:
    with open('config/config.yml', 'r') as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)

    return yaml_object


def get_all_ids_from_config() -> List[str]:
    config = read_config()

    return list(config['dashboards'].keys())


def load_override_tables(config: Dict, id: str) -> None:
    movie_overrides = config['dashboards'][id]['movie_multiplier_overrides']
    round_overrides = config['dashboards'][id]['round_multiplier_overrides']

    duckdb_con = DuckDBConnection(config, id)

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


def get_config(id: str) -> Dict:
    config = read_config()

    if config['dashboards'][id] is None:
        raise ValueError(
            f'Config ID {id} does not match config file {config["dashboards"]}'
        )

    load_override_tables(config, id)

    return config
