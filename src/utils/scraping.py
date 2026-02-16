import logging
import time

import requests
from bs4 import BeautifulSoup

BOX_OFFICE_MOJO_UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

DEFAULT_REQUEST_DELAY = 1.0
MAX_RETRIES = 3
INITIAL_BACKOFF = 2.0


def create_scrape_session() -> requests.Session:
    """Create a requests session with standard headers for Box Office Mojo."""
    session = requests.Session()
    session.headers.update(
        {
            'User-Agent': BOX_OFFICE_MOJO_UA,
            'Accept-Language': 'en-US,en;q=0.9',
        }
    )
    return session


def get_soup(
    session: requests.Session,
    url: str,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
) -> BeautifulSoup:
    """Fetch a URL and return parsed HTML with retry + exponential backoff on 503s."""
    for attempt in range(max_retries + 1):
        r = session.get(url, timeout=30)
        if r.status_code != 503 or attempt == max_retries:
            r.raise_for_status()
            return BeautifulSoup(r.text, 'lxml')

        wait = initial_backoff * (2**attempt)
        logging.warning(
            f'503 from {url}, retrying in {wait:.1f}s '
            f'(attempt {attempt + 1}/{max_retries})'
        )
        time.sleep(wait)

    # Should not reach here, but satisfy type checker
    r.raise_for_status()
    return BeautifulSoup(r.text, 'lxml')
