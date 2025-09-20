import logging

from gspread import Worksheet
from pandas import DataFrame

from src.utils.logging_config import setup_logging

setup_logging()


def df_to_sheet(
    df: DataFrame, worksheet: Worksheet, location: str, format_dict=None
) -> None:
    worksheet.update(
        range_name=location, values=[df.columns.values.tolist()] + df.values.tolist()
    )

    logging.info(
        f'Updated {location} with {df.shape[0]} rows and {df.shape[1]} columns'
    )

    if format_dict:
        for format_location, format_rules in format_dict.items():
            worksheet.format(ranges=format_location, format=format_rules)

        logging.info(f'Formatted {location} with {format_dict.keys()}')
