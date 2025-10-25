from typing import Dict, List, Optional

from pandas import DataFrame

from src.utils.db_connection import DuckDBConnection


def table_to_df(
    config: Dict,
    table: str,
    columns: Optional[List[str]] = None,
) -> DataFrame:
    duckdb_con = DuckDBConnection(config)

    df = duckdb_con.query(f'select * from {table}').df()

    duckdb_con.close()

    if columns:
        df.columns = columns

    df = df.replace([float('inf'), float('-inf'), float('nan')], None)

    return df
