import json
import os
import typing as t
from datetime import datetime

import pandas as pd
from gspread import service_account_from_dict
from pandas import DataFrame
from sqlmesh import ExecutionContext, model


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
    # Get credentials from environment variables (same as extract.py)
    gspread_credentials_name = os.getenv('GSPREAD_CREDENTIALS_NAME')
    sheet_name = os.getenv('GSPREAD_SHEET_NAME')

    if not gspread_credentials_name or not sheet_name:
        raise ValueError(
            "GSPREAD_CREDENTIALS_NAME and GSPREAD_SHEET_NAME must be set as environment variables"
        )

    # Get credentials from environment variable (same pattern as extract.py)
    credentials_dict = json.loads(
        os.getenv(gspread_credentials_name).replace('\n', '\\n')
    )
    gc = service_account_from_dict(credentials_dict)
    worksheet = gc.open(sheet_name).worksheet('Draft')

    raw = worksheet.get_all_values()
    df = DataFrame(data=raw[1:], columns=raw[0]).astype(str)

    return df
