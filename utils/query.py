from typing import List, Optional

from pandas import DataFrame

from utils.db_connection import DuckDBConnection


def temp_table_to_df(table: str, columns: Optional[List[str]] = None) -> DataFrame:
    duckdb_con = DuckDBConnection()

    df = duckdb_con.query(f'select * from {table}').df()

    if columns:
        df.columns = columns

    df = df.replace([float('inf'), float('-inf'), float('nan')], None)

    return df


def query_to_str(query_location: str) -> str:
    with open(query_location, 'r') as file:
        query = file.read().replace('\n', ' ').replace('\t', ' ')

    return query
