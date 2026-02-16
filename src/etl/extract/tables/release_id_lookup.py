import datetime
import logging
import ssl
import time
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd

from src.etl.extract.runner import run_extract
from src.utils.s3_utils import load_df_to_s3_parquet
from src.utils.scraping import DEFAULT_REQUEST_DELAY, create_scrape_session, get_soup

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = '%Y-%m-%d'
EXPECTED_COLUMNS = {'movie_title', 'release_group_url', 'domestic_release_url'}

BOX_OFFICE_MOJO_BASE = 'https://www.boxofficemojo.com'

_scrape_session = create_scrape_session()


def canonicalize(url: str) -> str:
    if not url:
        return None

    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc, p.path, '', ''))


def _year_world_releasegroup_records(
    year_world_url: str,
) -> list[dict[str, str | None]]:
    soup = get_soup(_scrape_session, year_world_url)
    seen = set()
    records = []
    for a in soup.select('a[href^="/releasegroup/"]'):
        href = a.get('href')
        if not href:
            continue
        url = urljoin(BOX_OFFICE_MOJO_BASE, href)
        if url in seen:
            continue
        seen.add(url)
        records.append(
            {
                'release_group_url': url,
                'movie_title': a.get_text(' ', strip=True) or None,
            }
        )
    return records


def _releasegroup_to_domestic_release_url(releasegroup_url: str) -> str | None:
    soup = get_soup(_scrape_session, releasegroup_url)

    for a in soup.select('a[href^="/release/"]'):
        if a.get_text(strip=True) == 'Domestic':
            return urljoin(BOX_OFFICE_MOJO_BASE, a['href'])

    for a in soup.select('a[href^="/release/"]'):
        if a.get_text(' ', strip=True).startswith('Domestic'):
            return urljoin(BOX_OFFICE_MOJO_BASE, a['href'])

    logging.warning(f'Could not find Domestic release link on {releasegroup_url}')
    return None


def extract(year: int) -> pd.DataFrame:
    try:
        logging.info(f'Extracting release ID lookup data for {year}.')
        year_url = f'{BOX_OFFICE_MOJO_BASE}/year/world/{year}/'
        releasegroup_records = _year_world_releasegroup_records(year_url)

        num_rows = len(releasegroup_records)
        logging.info(f'Found {num_rows} release groups for {year}.')

        records = []
        for count, rg in enumerate(releasegroup_records, start=1):
            rg_url = rg['release_group_url']
            try:
                domestic_url = _releasegroup_to_domestic_release_url(rg_url)
            except Exception as e:
                logging.warning(f'Failed to get domestic URL for {rg_url}: {e}')
                domestic_url = ''
            finally:
                records.append(
                    {
                        'movie_title': rg['movie_title'],
                        'release_group_url': canonicalize(rg_url),
                        'domestic_release_url': canonicalize(domestic_url),
                    }
                )

            if count % 5 == 0:
                logging.info(f'Parsed {count}/{num_rows} rows')

            time.sleep(DEFAULT_REQUEST_DELAY)

        return pd.DataFrame(records)
    except Exception as e:
        logging.error(f'Failed for {year}: {e}')
        return pd.DataFrame()


def load(df: pd.DataFrame, year: int) -> int:
    if df.empty:
        logging.warning(f'No release ID lookup data found for {year}.')
        return 0
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        logging.warning(
            f'Release ID lookup data for {year} is missing columns: {missing}. '
            f'Got columns: {list(df.columns)}'
        )
        return 0
    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = (
        f'raw/release_id_lookup/release_year={year}/scraped_date={formatted_date}/data'
    )
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def process_year(year: int) -> tuple[int, list[str]]:
    """Extract and load release ID lookup data for a given year."""
    try:
        df = extract(year)
        rows = load(df, year)
        if rows == 0:
            return 0, [str(year)]
        return rows, []
    except Exception as e:
        logging.error(f'Failed for {year}: {e}')
        return 0, [str(year)]


def main() -> None:
    run_extract('release_id_lookup', process_year)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    df = extract(datetime.date.today().year)
    print(df)
