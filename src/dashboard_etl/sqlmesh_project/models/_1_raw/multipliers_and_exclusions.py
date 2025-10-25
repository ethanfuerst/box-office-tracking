import os
import typing as t
from datetime import datetime

import pandas as pd
from pandas import DataFrame
from sqlmesh import ExecutionContext, model

from utils.gspread_utils import get_worksheet


@model(
    'raw.multipliers_and_exclusions',
    columns={
        'value': 'text',
        'multiplier': 'double',
        'type': 'text',
    },
    column_descriptions={
        'value': 'Value of the record',
        'multiplier': 'Multiplier of the record',
        'type': 'Type of the record',
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    sheet_name = os.getenv('GSPREAD_SHEET_NAME')

    if not sheet_name:
        raise ValueError('GSPREAD_SHEET_NAME must be set as environment variable')

    worksheet = get_worksheet(sheet_name, 'Multipliers and Exclusions')

    raw = worksheet.get_all_values()
    df = DataFrame(data=raw[1:], columns=raw[0]).astype(str)
    df['multiplier'] = df['multiplier'].replace('', 0).astype(float)

    return df
