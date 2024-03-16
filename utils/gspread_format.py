import pandas as pd
import gspread
from dotenv import load_dotenv

load_dotenv()


def df_to_sheet(df, worksheet, location, format_dict=None) -> None:
    worksheet.update(
        range_name=location, values=[df.columns.values.tolist()] + df.values.tolist()
    )
    print(f"Updated {location} with {df.shape[0]} rows and {df.shape[1]} columns")

    if format_dict:
        for format_location, format_rules in format_dict.items():
            worksheet.format(ranges=format_location, format=format_rules)
    return
