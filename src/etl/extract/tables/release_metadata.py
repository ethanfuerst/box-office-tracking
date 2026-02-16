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
from src.utils.scraping import DEFAULT_REQUEST_DELAY, create_scrape_session, get_soup

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = '%Y-%m-%d'
REQUIRED_COLUMNS = {'release_id'}
OPTIONAL_COLUMNS = {
    'movie_title',
    'distributor',
    'opening_amount',
    'opening_theaters',
    'release_date',
    'rating',
    'runtime',
    'genres',
    'widest_release',
}

_scrape_session = create_scrape_session()


def _clean_currency(val: str) -> str | None:
    if not val or val in ['-', '–', '—', 'N/A']:
        return None
    val = re.sub(r'[^\d,]', '', val)
    val = val.replace(',', '')
    return val if val else None


def _clean_number(val: str) -> str | None:
    if not val or val in ['-', '–', '—', 'N/A']:
        return None
    val = re.sub(r'[^\d,]', '', val)
    val = val.replace(',', '')
    return val if val else None


def _parse_opening(opening_text: str) -> tuple[str | None, str | None]:
    """Parse opening text like '$100,262,540 4,000 theaters' into amount and theater count."""
    if not opening_text:
        return None, None

    # Clean up whitespace
    opening_text = ' '.join(opening_text.split())
    parts = opening_text.split()
    amount = None
    theaters = None

    for i, part in enumerate(parts):
        if part.startswith('$'):
            amount = _clean_currency(part)
        if 'theater' in part.lower() and i > 0:
            theaters = _clean_number(parts[i - 1])

    return amount, theaters


def _scrape_release(release_id: str) -> pd.DataFrame:
    """Scrape metadata from a Box Office Mojo release page."""
    release_url = f'https://www.boxofficemojo.com/release/{release_id}/'
    try:
        soup = get_soup(_scrape_session, release_url)

        metadata = {'release_id': release_id}

        # Extract movie title
        title_elem = soup.find('h1', class_='a-size-extra-large')
        if title_elem:
            metadata['movie_title'] = title_elem.get_text(strip=True)

        # Find the summary table
        summary_divs = soup.find_all('div', class_='a-section a-spacing-none')

        for div in summary_divs:
            spans = div.find_all('span')
            if len(spans) >= 2:
                label = spans[0].get_text(strip=True).lower().replace(':', '')
                value = spans[1].get_text(' ', strip=True)

                if 'distributor' in label:
                    # Remove "See full company information" suffix
                    value = value.replace('See full company information', '').strip()
                    metadata['distributor'] = value
                elif 'opening' in label:
                    amount, theaters = _parse_opening(value)
                    metadata['opening_amount'] = amount
                    metadata['opening_theaters'] = theaters
                elif 'release date' in label:
                    metadata['release_date'] = value
                elif 'mpaa' in label:
                    metadata['rating'] = value
                elif 'running time' in label:
                    metadata['runtime'] = value
                elif 'genres' in label:
                    # Clean up whitespace in genres
                    metadata['genres'] = ' '.join(value.split())
                elif 'widest release' in label:
                    # Extract just the number
                    metadata['widest_release'] = _clean_number(value.split()[0])

        return pd.DataFrame([metadata])
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

    # Check required columns only
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        logging.warning(
            f'Data for {release_id} is missing required columns: {missing_required}. '
            f'Got columns: {list(df.columns)}'
        )
        return 0

    # Log if optional columns are missing (for debugging)
    missing_optional = OPTIONAL_COLUMNS - set(df.columns)
    if missing_optional:
        logging.debug(
            f'Data for {release_id} is missing optional columns: {missing_optional}'
        )

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = (
        f'raw/release_metadata/release_id={release_id}/'
        f'scraped_date={formatted_date}/data'
    )
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def process_year(year: int) -> tuple[int, list[str]]:
    """
    Process all releases for a given year.

    Returns:
        tuple: (total_rows_loaded, list_of_failed_ids)
    """
    logging.info(f'Processing release metadata for {year}.')
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
    run_extract('release_metadata', process_year, years=years)


if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    current_year = datetime.date.today().year
    rows, failed = process_year(current_year)
    print(f'\\nTotal rows: {rows}')
    if failed:
        print(f'Failed releases: {failed}')
