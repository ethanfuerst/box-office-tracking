import datetime
import logging
import ssl
import time
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.s3_utils import load_df_to_s3_parquet

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = '%Y-%m-%d'

BOX_OFFICE_MOJO_BASE = 'https://www.boxofficemojo.com'
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


def canonicalize(url: str) -> str:
    if not url:
        return None

    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc, p.path, '', ''))


def _get_soup(url: str) -> BeautifulSoup:
    r = _scrape_session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, 'lxml')


def _year_world_releasegroup_records(
    year_world_url: str,
) -> list[dict[str, str | None]]:
    soup = _get_soup(year_world_url)
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
    soup = _get_soup(releasegroup_url)

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

            time.sleep(0.25)

        return pd.DataFrame(records)
    except Exception as e:
        logging.error(f'Failed for {year}: {e}')
        return pd.DataFrame()


def load(df: pd.DataFrame, year: int) -> int:
    if df.empty:
        logging.warning(f'No release ID lookup data found for {year}.')
        return 0
    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)
    s3_key = (
        f'raw/release_id_lookup/release_year={year}/scraped_date={formatted_date}/data'
    )
    return load_df_to_s3_parquet(df=df, s3_key=s3_key)


def main() -> None:
    logging.info('Extracting release ID lookup data.')
    current_year = datetime.date.today().year
    total_rows = 0
    for year in [current_year, current_year - 1]:
        try:
            df = extract(year)
            total_rows += load(df, year)
        except Exception as e:
            logging.error(f'Failed for {year}: {e}')
    logging.info(f'Total rows loaded: {total_rows}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    df = extract(datetime.date.today().year)
    print(df)
