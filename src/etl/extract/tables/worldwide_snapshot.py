import datetime
import logging
import re
import ssl
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from src.utils.s3_utils import get_df_from_s3_parquet, load_df_to_s3_parquet

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = '%Y-%m-%d'
EXPECTED_COLUMNS = {
    'movie_title',
    'region',
    'market',
    'release_date',
    'opening',
    'total_gross',
    'release_group_url',
}

BOX_OFFICE_MOJO_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

_scrape_session = requests.Session()
_scrape_session.headers.update(
    {
        'User-Agent': BOX_OFFICE_MOJO_UA,
        'Accept-Language': 'en-US,en;q=0.9',
    }
)


def _get_soup(url: str) -> BeautifulSoup:
    r = _scrape_session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, 'lxml')


def _clean_currency(val: str) -> str | None:
    if not val or val in ['-', '–', '—', 'N/A']:
        return None
    val = re.sub(r'[^\d,]', '', val)
    val = val.replace(',', '')
    return val if val else None


def _clean_date(val: str) -> str | None:
    if not val or val in ['-', '–', '—', 'N/A']:
        return None
    val = val.strip()
    return val if val else None


def _parse_regional_table(
    region_name: str, table: BeautifulSoup
) -> list[dict[str, str | None]]:
    records = []
    rows = table.select('tr')

    for row in rows[1:]:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            continue

        market = cells[0].get_text(' ', strip=True)
        if not market or market.lower() in ['market', 'total', 'summary']:
            continue

        release_date = None
        opening = None
        total_gross = None

        if len(cells) >= 3:
            release_date = _clean_date(cells[1].get_text(strip=True))
            opening = _clean_currency(cells[2].get_text(strip=True))

        if len(cells) >= 4:
            total_gross = _clean_currency(cells[3].get_text(strip=True))
        elif len(cells) == 3:
            total_gross = opening
            opening = None

        records.append(
            {
                'region': region_name,
                'market': market,
                'release_date': release_date,
                'opening': opening,
                'total_gross': total_gross,
            }
        )

    return records


def _scrape_releasegroup(release_group_id: str) -> pd.DataFrame:
    """Scrape regional box office data for a release group."""
    release_group_url = (
        f'https://www.boxofficemojo.com/releasegroup/{release_group_id}/'
    )
    try:
        soup = _get_soup(release_group_url)
        all_records = []

        movie_title = None
        title_elem = soup.find('h1', class_='a-size-extra-large')
        if title_elem:
            movie_title = title_elem.get_text(strip=True)

        tables = soup.find_all('table', class_='releases-by-region')
        for table in tables:
            region_header = table.find('th', attrs={'colspan': '4'})
            if not region_header:
                continue

            region_text = region_header.get_text(strip=True)
            if not region_text or region_text.lower() == 'worldwide':
                continue

            records = _parse_regional_table(region_text, table)
            for rec in records:
                rec['movie_title'] = movie_title
                rec['release_group_url'] = release_group_url
            all_records.extend(records)

        return pd.DataFrame(all_records)
    except Exception as e:
        logging.warning(f'Failed to scrape {release_group_id}: {e}')
        return pd.DataFrame()


def _get_release_ids_from_s3(year: int) -> list[str]:
    """Get release group IDs for a given year from S3."""
    try:
        s3_path = f'raw/release_id_lookup/release_year={year}/**/*.parquet'
        df = get_df_from_s3_parquet(s3_path)
        urls = df['release_group_url'].drop_duplicates().tolist()
        return [_extract_release_group_id(url) for url in urls]
    except Exception as e:
        logging.warning(f'Could not read release_id_lookup for {year}: {e}')
        return []


def _extract_release_group_id(url: str) -> str:
    """Extract release group ID from URL"""
    match = re.search(r'/releasegroup/(gr\d+)/', url)
    if match:
        return match.group(1)
    return url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]


def load(df: pd.DataFrame, release_group_id: str) -> int:
    """Load a DataFrame to S3 partitioned by release_group_id and scraped_date."""
    if df.empty:
        logging.debug(f'No data to load for {release_group_id}')
        return 0
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        logging.warning(
            f'Data for {release_group_id} is missing columns: {missing}. '
            f'Got columns: {list(df.columns)}'
        )
        return 0

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = (
        f'raw/worldwide_snapshot/release_group_id={release_group_id}/'
        f'scraped_date={formatted_date}/data'
    )
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def process_year(year: int) -> tuple[int, list[str]]:
    """
    Process all release groups for a given year.

    Returns:
        tuple: (total_rows_loaded, list_of_failed_ids)
    """
    logging.info(f'Processing worldwide snapshot data for {year}.')
    release_group_ids = _get_release_ids_from_s3(year)

    if not release_group_ids:
        logging.warning(f'No release groups found for {year}.')
        return 0, []

    logging.info(f'Found {len(release_group_ids)} release groups for {year}.')

    total_rows = 0
    failed_release_groups = []

    for count, release_group_id in enumerate(release_group_ids, start=1):
        logging.info(f'Processing {count}/{len(release_group_ids)}: {release_group_id}')
        try:
            df = _scrape_releasegroup(release_group_id)
            rows = load(df, release_group_id)
            total_rows += rows
            if rows > 0:
                logging.debug(f'Loaded {rows} rows for {release_group_id}')
        except Exception as e:
            logging.error(f'Failed to process {release_group_id}: {e}')
            failed_release_groups.append(release_group_id)

        time.sleep(0.5)

    logging.info(f'Loaded {total_rows} rows for {year}.')
    return total_rows, failed_release_groups


def main() -> None:
    logging.info('Extracting worldwide snapshot data.')
    current_year = datetime.date.today().year

    total_rows = 0
    all_failed_release_groups = []

    for year in [current_year, current_year - 1]:
        rows, failed = process_year(year)
        total_rows += rows
        all_failed_release_groups.extend(failed)

    logging.info(f'Total rows loaded across all years: {total_rows}')

    if all_failed_release_groups:
        logging.error(f'{len(all_failed_release_groups)} release groups failed.')
        raise RuntimeError(
            f'worldwide_snapshot extract failed for {len(all_failed_release_groups)} '
            f'release group IDs: {all_failed_release_groups[:10]}'
        )


if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    current_year = datetime.date.today().year
    rows, failed = process_year(current_year)
    print(f'\nTotal rows: {rows}')
    if failed:
        print(f'Failed release groups: {failed}')
