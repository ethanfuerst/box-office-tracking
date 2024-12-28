from typing import List, Optional

from pandas import DataFrame

from utils.db_connection import DuckDBConnection


def temp_table_to_df(
    table: str, db_con: DuckDBConnection, columns: Optional[List[str]] = None
) -> DataFrame:
    df = db_con.query(f"select * from {table}").df()
    if columns:
        df.columns = columns

    return df


def query_to_str(query_location: str) -> str:
    with open(query_location, "r") as file:
        query = file.read().replace("\n", " ").replace("\t", " ")
    return query
