import datetime
import logging

import pandas as pd
from pandas import read_html

from src.utils.s3_utils import load_df_to_s3_parquet

S3_DATE_FORMAT = '%Y-%m-%d'


def extract(start_year: int | None = None, end_year: int | None = None) -> pd.DataFrame:
    current_year = datetime.date.today().year
    if start_year is None and end_year is None:
        years = [current_year, current_year - 1]
    else:
        start = start_year if start_year is not None else 1977
        end = end_year if end_year is not None else current_year
        years = list(range(start, end + 1))

    dfs = []
    for year in years:
        try:
            logging.info(f'Extracting worldwide box office data for {year}.')
            url = f'https://www.boxofficemojo.com/year/world/{year}'
            dfs.append(read_html(url)[0])
        except Exception as e:
            logging.error(f'Failed for {year}: {e}')
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load(df: pd.DataFrame, year: int) -> int:
    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = f'raw/worldwide_box_office/release_year={year}/scraped_date={formatted_date}/data'
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def main() -> None:
    logging.info('Extracting worldwide box office data.')
    current_year = datetime.date.today().year
    total_rows = 0
    for year in [current_year, current_year - 1]:
        try:
            df = extract(start_year=year, end_year=year)
            total_rows += load(df, year)
        except Exception as e:
            logging.error(f'Failed for {year}: {e}')
    logging.info(f'Total rows loaded: {total_rows}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    df = extract()
    print(df)
