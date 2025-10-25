import os
import ssl
import typing as t
from datetime import datetime

import pandas as pd
from pandas import read_html
from sqlmesh import ExecutionContext, model

from src.utils.db_connection import DuckDBConnection


@model(
    'raw.box_office_mojo_dump',
    kind='FULL',
    columns={
        'title': 'text',
        'revenue': 'int',
        'domestic_rev': 'int',
        'foreign_rev': 'int',
        'loaded_date': 'date',
        'year_part': 'text',
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    update_type = os.getenv('UPDATE_TYPE', 's3')
    year = int(os.getenv('YEAR', 2025))
    bucket = os.getenv('BUCKET', '')

    if update_type == 's3':
        s3_query = f'''
            with all_data as (
                select
                    *
                    , split_part(split_part(filename, 'release_year=', 2), '/', 1) as year_part_from_s3
                    , strptime(split_part(split_part(filename, 'scraped_date=', 2), '/', 1), '%Y-%m-%d') as scraped_date_from_s3
                from read_parquet('s3://{bucket}/release_year=*/scraped_date=*/data.parquet', filename=true)
            )
            select
                "Release Group" as title
                , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                , scraped_date_from_s3 as loaded_date
                , year_part_from_s3 as year_part
            from all_data
        '''

        result_df = context.engine_adapter.fetchdf(s3_query)
    else:
        ssl._create_default_https_context = ssl._create_unverified_context

        df = read_html(f'https://www.boxofficemojo.com/year/world/{year}')[0]

        result_df = df.copy()
        result_df['title'] = result_df['Release Group']
        result_df['revenue'] = (
            result_df['Worldwide']
            .str[2:]
            .str.replace(',', '')
            .astype(int, errors='coerce')
            .fillna(0)
        )
        result_df['domestic_rev'] = (
            result_df['Domestic']
            .str[2:]
            .str.replace(',', '')
            .astype(int, errors='coerce')
            .fillna(0)
        )
        result_df['foreign_rev'] = (
            result_df['Foreign']
            .str[2:]
            .str.replace(',', '')
            .astype(int, errors='coerce')
            .fillna(0)
        )
        result_df['loaded_date'] = pd.Timestamp.now().date()
        result_df['year_part'] = str(year)

        result_df = result_df[
            [
                'title',
                'revenue',
                'domestic_rev',
                'foreign_rev',
                'loaded_date',
                'year_part',
            ]
        ].copy()

    return result_df
