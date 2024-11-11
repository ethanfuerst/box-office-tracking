import pandas as pd


def temp_table_to_df(table, db_con, columns=None) -> pd.DataFrame:
    df = db_con.query(f"select * from {table}").df()
    if columns:
        df.columns = columns

    return df
