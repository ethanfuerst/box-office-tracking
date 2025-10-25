import json
import os
from typing import Dict, List

import yaml
from gspread import service_account_from_dict
from pandas import DataFrame

from src.utils.db_connection import DuckDBConnection


def get_top_level_config() -> Dict:
    with open('src/config/config.yml', 'r') as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)

    return yaml_object


def get_all_ids_from_config() -> List[str]:
    config = get_top_level_config()

    return list(config['dashboards'].keys())


def load_override_tables(config: Dict) -> None:
    gspread_credentials_name = config.get(
        'gspread_credentials_name', f'GSPREAD_CREDENTIALS_{config["year"]}'
    )

    credentials_dict = json.loads(
        os.getenv(gspread_credentials_name).replace('\n', '\\n')
    )
    gc = service_account_from_dict(credentials_dict)

    worksheet = gc.open(config['sheet_name']).worksheet('Multipliers and Exclusions')
    raw_multipliers_and_exclusions = worksheet.get_all_values()

    df_multipliers_and_exclusions = DataFrame(
        data=raw_multipliers_and_exclusions[1:],
        columns=raw_multipliers_and_exclusions[0],
    ).astype(str)

    worksheet = gc.open(config['sheet_name']).worksheet('Manual Adds')
    raw_manual_adds = worksheet.get_all_values()

    df_manual_adds = DataFrame(
        data=raw_manual_adds[1:],
        columns=raw_manual_adds[0],
    ).astype(str)

    duckdb_con = DuckDBConnection(config)

    duckdb_con.connection.register(
        'df_multipliers_and_exclusions', df_multipliers_and_exclusions
    )
    duckdb_con.execute(
        'create or replace table raw_multipliers_and_exclusions as select * from df_multipliers_and_exclusions;'
    )

    duckdb_con.connection.register('df_manual_adds', df_manual_adds)
    duckdb_con.execute(
        'create or replace table raw_manual_adds as select * from df_manual_adds;'
    )

    duckdb_con.execute(
        '''
        create or replace table movie_multiplier_overrides as (
            select
                try_cast(value as varchar) as movie
                , try_cast(multiplier as double) as multiplier
            from raw_multipliers_and_exclusions
            where try_cast(type as varchar) = 'movie'
        );
        '''
    )

    duckdb_con.execute(
        '''
        create or replace table round_multiplier_overrides as (
            select
                try_cast(value as varchar) as round
                , try_cast(multiplier as double) as multiplier
            from raw_multipliers_and_exclusions
            where try_cast(type as varchar) = 'round'
        );
        '''
    )

    duckdb_con.execute(
        '''
        create or replace table draft_year_exclusions as (
            select
                try_cast(value as varchar) as movie
            from raw_multipliers_and_exclusions
            where try_cast(type as varchar) = 'exclusion'
        );
        '''
    )

    duckdb_con.execute(
        '''
        create or replace table manual_adds as (
            select
                try_cast(title as varchar) as title
                , try_cast(revenue as integer) as revenue
                , try_cast(domestic_rev as integer) as domestic_rev
                , try_cast(foreign_rev as integer) as foreign_rev
                , try_cast(release_date as date) as first_seen_date
            from raw_manual_adds
        );
        '''
    )

    duckdb_con.close()


def get_config_for_id(id: str) -> Dict:
    top_level_config = get_top_level_config()

    config = top_level_config['dashboards'][id]
    config['dashboard_id'] = id

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
