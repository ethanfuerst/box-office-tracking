import datetime
import logging
import re
import ssl
import time

import pandas as pd
from dotenv import load_dotenv

from src.etl.extract.runner import run_extract
from src.utils.s3_utils import (
    find_latest_partition,
    get_df_from_s3_parquet,
    load_df_to_s3_parquet,
)
from src.utils.scraping import BOX_OFFICE_MOJO_UA, DEFAULT_REQUEST_DELAY

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = '%Y-%m-%d'


def _scrape_release(release_id: str) -> pd.DataFrame:
    """Scrape daily box office data from a Box Office Mojo release page."""
    release_url = f'https://www.boxofficemojo.com/release/{release_id}/'
    try:
        # Use pandas read_html to parse all tables on the page
        tables = pd.read_html(
            release_url,
            storage_options={'User-Agent': BOX_OFFICE_MOJO_UA},
        )

        if not tables:
            logging.warning(f'No tables found for {release_id}')
            return pd.DataFrame()

        # Get the last table (the "bottom one" as specified in the ticket)
        df = tables[-1]

        # Add release_id column
        df['release_id'] = release_id

        return df
    except Exception as e:
        logging.warning(f'Failed to scrape {release_id}: {e}')
        return pd.DataFrame()


def _get_release_ids_from_s3(year: int) -> list[str]:
    """Get release IDs for a given year from S3."""
    try:
        partition = find_latest_partition(f'raw/release_id_lookup/release_year={year}')
        if not partition:
            logging.warning(f'No release_id_lookup partitions for {year}')
            return []
        df = get_df_from_s3_parquet(f'{partition}/*.parquet')
        # Filter out empty/null URLs
        urls = df['domestic_release_url'].dropna().drop_duplicates().tolist()
        urls = [url for url in urls if url and url.strip()]
        return [_extract_release_id(url) for url in urls]
    except Exception as e:
        logging.warning(f'Could not read release_id_lookup for {year}: {e}')
        return []


def _extract_release_id(url: str) -> str:
    """Extract release ID from URL."""
    match = re.search(r'/release/(rl\d+)/', url)
    if match:
        return match.group(1)
    return url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]


def load(df: pd.DataFrame, release_id: str) -> int:
    """Load a DataFrame to S3 partitioned by release_id and scraped_date."""
    if df.empty:
        logging.debug(f'No data to load for {release_id}')
        return 0

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = (
        f'raw/release_domestic/release_id={release_id}/'
        f'scraped_date={formatted_date}/data'
    )
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def process_year(year: int) -> tuple[int, list[str]]:
    """
    Process all releases for a given year.

    Returns:
        tuple: (total_rows_loaded, list_of_failed_ids)
    """
    logging.info(f'Processing release domestic data for {year}.')
    release_ids = _get_release_ids_from_s3(year)

    if not release_ids:
        logging.warning(f'No releases found for {year}.')
        return 0, []

    logging.info(f'Found {len(release_ids)} releases for {year}.')

    total_rows = 0
    failed_releases = []

    for count, release_id in enumerate(release_ids, start=1):
        logging.info(f'Processing {count}/{len(release_ids)}: {release_id}')
        try:
            df = _scrape_release(release_id)
            rows = load(df, release_id)
            total_rows += rows
            if rows > 0:
                logging.debug(f'Loaded {rows} rows for {release_id}')
        except Exception as e:
            logging.error(f'Failed to process {release_id}: {e}')
            failed_releases.append(release_id)

        time.sleep(DEFAULT_REQUEST_DELAY)

    logging.info(f'Loaded {total_rows} rows for {year}.')
    return total_rows, failed_releases


def main(years: list[int] | None = None) -> None:
    run_extract('release_domestic', process_year, years=years)


if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    current_year = datetime.date.today().year
    rows, failed = process_year(current_year)
    print(f'\nTotal rows: {rows}')
    if failed:
        print(f'Failed releases: {failed}')
