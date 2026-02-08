import datetime
import logging

from src.etl.extract.tables import (
    release_domestic,
    release_id_lookup,
    release_metadata,
    worldwide_box_office,
    worldwide_snapshot,
)
from src.utils.logging_config import setup_logging

setup_logging()


def main(force_all: bool = False) -> None:
    logging.info('Starting extraction pipeline.')

    errors = []

    try:
        worldwide_box_office.main()
    except Exception as e:
        logging.error(f'worldwide_box_office failed: {e}')
        errors.append(('worldwide_box_office', e))

    if force_all or datetime.date.today().weekday() == 2:  # force_all or Wednesday
        try:
            release_id_lookup.main()
        except Exception as e:
            logging.error(f'release_id_lookup failed: {e}')
            errors.append(('release_id_lookup', e))

        try:
            release_metadata.main()
        except Exception as e:
            logging.error(f'release_metadata failed: {e}')
            errors.append(('release_metadata', e))

        try:
            release_domestic.main()
        except Exception as e:
            logging.error(f'release_domestic failed: {e}')
            errors.append(('release_domestic', e))

        try:
            worldwide_snapshot.main()
        except Exception as e:
            logging.error(f'worldwide_snapshot failed: {e}')
            errors.append(('worldwide_snapshot', e))

    logging.info('Extraction pipeline complete.')

    if errors:
        failed = ', '.join(name for name, _ in errors)
        raise RuntimeError(f'Extraction failed for: {failed}')
