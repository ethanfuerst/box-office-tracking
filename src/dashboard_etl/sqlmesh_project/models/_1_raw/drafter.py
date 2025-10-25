import os
import typing as t
from datetime import datetime

import pandas as pd
from pandas import DataFrame
from sqlmesh import ExecutionContext, model

from utils.gspread_utils import get_worksheet


@model(
    'raw.drafter',
    owner='janet',
    cron='@daily',
    columns={
        'round': 'int',
        'overall_pick': 'int',
        'name': 'text',
        'movie': 'text',
    },
    column_descriptions={
        'round': 'Round number of draft',
        'overall_pick': 'Overall pick number of draft',
        'name': 'Name of the person picking',
        'movie': 'Movie picked',
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

    worksheet = get_worksheet(sheet_name, 'Draft')

    raw = worksheet.get_all_values()
    df = DataFrame(data=raw[1:], columns=raw[0]).astype(str)

    return df
