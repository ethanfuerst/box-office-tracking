import datetime
import logging

from src.etl.extract.tables import release_id_lookup, worldwide_box_office
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

    logging.info('Extraction pipeline complete.')

    if errors:
        failed = ', '.join(name for name, _ in errors)
        raise RuntimeError(f'Extraction failed for: {failed}')
