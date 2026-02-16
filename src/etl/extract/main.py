import datetime
import logging

from src.etl.extract.tables import (
    release_domestic,
    release_id_lookup,
    release_metadata,
    release_worldwide_snapshot,
    worldwide_box_office,
)
from src.utils.logging_config import setup_logging

setup_logging()

EXTRACT_MODULES = {
    'worldwide_box_office': worldwide_box_office,
    'release_id_lookup': release_id_lookup,
    'release_metadata': release_metadata,
    'release_domestic': release_domestic,
    'release_worldwide_snapshot': release_worldwide_snapshot,
}

DAILY_EXTRACTS = ['worldwide_box_office', 'release_id_lookup']
WEEKLY_SCHEDULE = {
    1: 'release_domestic',  # Tuesday
    2: 'release_metadata',  # Wednesday
    3: 'release_worldwide_snapshot',  # Thursday
}
ALL_EXTRACTS = DAILY_EXTRACTS + list(WEEKLY_SCHEDULE.values())


def main(
    extract_names: list[str] | None = None,
    years: list[int] | None = None,
) -> list[tuple[str, Exception]]:
    """Run extraction pipeline.

    Args:
        extract_names: Specific extracts to run. Use ['all'] to run everything.
            If None, runs based on schedule (daily + one weekly per day).
        years: Explicit list of years to process. If None, each module
            uses its default (current year and previous year).

    Returns:
        List of (extract_name, exception) tuples for any failures.
    """
    logging.info('Starting extraction pipeline.')

    if extract_names:
        if 'all' in extract_names:
            extracts_to_run = ALL_EXTRACTS
        else:
            unknown = set(extract_names) - set(EXTRACT_MODULES)
            if unknown:
                raise ValueError(
                    f'Unknown extract(s): {unknown}. '
                    f'Available: {list(EXTRACT_MODULES.keys())}'
                )
            extracts_to_run = extract_names
    else:
        weekday = datetime.date.today().weekday()
        weekly_extract = WEEKLY_SCHEDULE.get(weekday)
        extracts_to_run = DAILY_EXTRACTS + ([weekly_extract] if weekly_extract else [])

    errors = []
    for name in extracts_to_run:
        try:
            EXTRACT_MODULES[name].main(years=years)
        except Exception as e:
            logging.error(f'{name} failed: {e}')
            errors.append((name, e))

    logging.info('Extraction pipeline complete.')

    if errors:
        failed = ', '.join(name for name, _ in errors)
        logging.warning(f'Extraction had failures: {failed}')

    return errors
