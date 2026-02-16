import datetime
import logging
from collections.abc import Callable


def run_extract(
    name: str,
    process_year: Callable[[int], tuple[int, list[str]]],
) -> None:
    """Shared runner for extract modules.

    Iterates over current and previous year, collects failures,
    and raises if any occurred.

    Args:
        name: Extract name (used in log/error messages).
        process_year: Function that takes a year and returns
            (rows_loaded, list_of_failed_ids).
    """
    logging.info(f'Extracting {name} data.')
    current_year = datetime.date.today().year

    total_rows = 0
    all_failed = []

    for year in [current_year, current_year - 1]:
        rows, failed = process_year(year)
        total_rows += rows
        all_failed.extend(failed)

    logging.info(f'{name}: loaded {total_rows} total rows.')

    if all_failed:
        logging.error(f'{name}: {len(all_failed)} items failed.')
        raise RuntimeError(
            f'{name} extract failed for {len(all_failed)} items: {all_failed[:10]}'
        )
