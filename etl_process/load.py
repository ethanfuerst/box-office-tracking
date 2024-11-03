import os
import json
import pandas as pd
import datetime
import logging
import time
import gspread
import gspread_formatting as gsf
from utils.db_connection import DuckDBConnection
from utils.format import load_format_config
from utils.query import query_to_str, temp_table_to_df
from utils.gspread_format import df_to_sheet
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def load() -> None:
    duckdb_con = DuckDBConnection()

    released_movies_df = temp_table_to_df(
        "base_query",
        duckdb_con,
        columns=[
            "Rank",
            "Title",
            "Drafted By",
            "Revenue",
            "Scored Revenue",
            "Round Drafted",
            "Overall Pick",
            "Multiplier",
            "Domestic Revenue",
            "Domestic Revenue %",
            "Foreign Revenue",
            "Foreign Revenue %",
            "Better Pick",
            "Better Pick Scored Revenue",
            "First Seen Date",
        ],
    )

    scoreboard_df = temp_table_to_df(
        "scoreboard",
        duckdb_con,
        columns=[
            "Name",
            "Scored Revenue",
            "# Released",
            "# Optimal Picks",
            "% Optimal Picks",
            "Unadjusted Revenue",
        ],
    )

    dashboard_elements = [
        (
            scoreboard_df,
            "B4",
            load_format_config("assets/scoreboard_format.json"),
        ),
        (
            released_movies_df,
            "I4",
            load_format_config("assets/released_movies_format.json"),
        ),
    ]

    worst_picks_df = temp_table_to_df(
        "worst_picks",
        duckdb_con,
        columns=[
            "Rank",
            "Title",
            "Drafted By",
            "Overall Pick",
            "Number of Better Picks",
            "Missed Revenue",
        ],
    )

    add_worst_picks = (
        len(released_movies_df) > len(scoreboard_df) + 3 and len(worst_picks_df) > 1
    )

    if add_worst_picks:
        worst_picks_df_height = len(released_movies_df) - len(scoreboard_df) - 3

        worst_picks_df = worst_picks_df.head(worst_picks_df_height)

        dashboard_elements.append(
            (
                worst_picks_df,
                "B12",
                load_format_config("assets/worst_picks_format.json"),
            )
        )

    gspread_credentials = os.getenv("GSPREAD_CREDENTIALS")
    if gspread_credentials is not None:
        credentials_dict = json.loads(gspread_credentials.replace("\n", "\\n"))
        gc = gspread.service_account_from_dict(credentials_dict)
    else:
        raise ValueError(
            "GSPREAD_CREDENTIALS is not set or is invalid in the .env file."
        )

    sh = gc.open("2024 Fantasy Box Office Draft")

    worksheet_title = "Dashboard"
    worksheet = sh.worksheet(worksheet_title)

    sh.del_worksheet(worksheet)
    # 3 rows for title, 1 row for column titles, 1 row for footer
    sheet_height = len(released_movies_df) + 5
    worksheet = sh.add_worksheet(
        title=worksheet_title, rows=sheet_height, cols=24, index=1
    )

    # Adding each dashboard element
    for element in dashboard_elements:
        df_to_sheet(
            df=element[0],
            worksheet=worksheet,
            location=element[1],
            format_dict=element[2] if len(element) > 2 else None,
        )

    # Adding last updated header
    worksheet.update(
        values=[
            [
                f"Last Updated UTC\n{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ]
        ],
        range_name="G2",
    )
    worksheet.format(
        "G2",
        {
            "horizontalAlignment": "CENTER",
        },
    )

    # Columns are created with 12 point font, then auto resized and reduced to 10 point bold font
    worksheet.columns_auto_resize(1, 7)
    worksheet.columns_auto_resize(8, 23)

    worksheet.format(
        "B4:G4",
        {
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
    )
    if add_worst_picks:
        worksheet.format(
            "B12:G12",
            {
                "horizontalAlignment": "CENTER",
                "textFormat": {
                    "fontSize": 10,
                    "bold": True,
                },
            },
        )
    worksheet.format(
        "I4:W4",
        {
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
    )

    for i in range(5, sheet_height):
        if worksheet.acell(f"V{i}").value == "$0":
            worksheet.update(values=[[""]], range_name=f"V{i}")

    # resizing spacer columns
    spacer_columns = ["A", "H", "X"]
    for column in spacer_columns:
        gsf.set_column_width(worksheet, column, 25)

    time.sleep(60)

    # for some reason the auto resize still cuts off some of the title
    title_columns = ["J", "U"]

    if add_worst_picks:
        title_columns.append("C")

    for column in title_columns:
        gsf.set_column_width(worksheet, column, 256)

    # revenue columns will also get cut off
    revenue_columns = ["L", "M", "R", "S"]
    for column in revenue_columns:
        gsf.set_column_width(worksheet, column, 120)

    # gets resized wrong and have to do it manually
    gsf.set_column_width(worksheet, "R", 142)
    gsf.set_column_width(worksheet, "W", 104)

    # Adding titles
    worksheet.update(values=[["Fantasy Box Office Standings"]], range_name="B2")
    worksheet.format(
        "B2",
        {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 20, "bold": True}},
    )
    worksheet.merge_cells("B2:F2")
    worksheet.update(values=[["Released Movies"]], range_name="I2")
    worksheet.format(
        "I2",
        {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 20, "bold": True}},
    )
    worksheet.merge_cells("I2:W2")

    if add_worst_picks:
        worksheet.update(values=[["Worst Picks"]], range_name="B11")
        worksheet.format(
            "B11",
            {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
        )
        worksheet.merge_cells("B11:G11")

    logger.info("Dashboard updated and formatted")
    duckdb_con.close()
