from logging import getLogger
from typing import Dict, List, Optional

from gspread import Worksheet
from pandas import DataFrame

logger = getLogger(__name__)


def df_to_sheet(
    df: DataFrame, worksheet: Worksheet, location: str, format_dict=None
) -> None:
    worksheet.update(
        range_name=location, values=[df.columns.values.tolist()] + df.values.tolist()
    )

    logger.info(f"Updated {location} with {df.shape[0]} rows and {df.shape[1]} columns")

    if format_dict:
        for format_location, format_rules in format_dict.items():
            worksheet.format(ranges=format_location, format=format_rules)

        logger.info(f"Formatted {location} with {format_dict.keys()}")
