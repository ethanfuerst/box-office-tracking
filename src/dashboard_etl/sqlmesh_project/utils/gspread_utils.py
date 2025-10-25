import json
import os

from gspread import service_account_from_dict


def get_gspread_client():
    '''Helper function to get Google Sheets client with credentials.'''
    gspread_credentials_name = os.getenv('GSPREAD_CREDENTIALS_NAME')

    if not gspread_credentials_name:
        raise ValueError('GSPREAD_CREDENTIALS_NAME must be set as environment variable')

    credentials_dict = json.loads(
        os.getenv(gspread_credentials_name).replace('\n', '\\n')
    )
    return service_account_from_dict(credentials_dict)


def get_worksheet(sheet_name: str, worksheet_name: str):
    '''Helper function to get a specific worksheet from a Google Sheet.'''
    gc = get_gspread_client()
    return gc.open(sheet_name).worksheet(worksheet_name)
