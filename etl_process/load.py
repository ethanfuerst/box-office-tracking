import datetime
import json
import os
import time
from logging import getLogger

import gspread
import gspread_formatting as gsf
import pandas as pd
from dotenv import load_dotenv

from utils.db_connection import DuckDBConnection
from utils.format import load_format_config
from utils.gspread_format import df_to_sheet
from utils.query import temp_table_to_df

load_dotenv()
logger = getLogger(__name__)


def load(year: int) -> None:
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
            "Still In Theaters",
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
    duckdb_con.close()

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

    gspread_credentials_key = f"GSPREAD_CREDENTIALS_{year}"
    gspread_credentials = os.getenv(gspread_credentials_key)
    if gspread_credentials is not None:
        credentials_dict = json.loads(gspread_credentials.replace("\n", "\\n"))
        gc = gspread.service_account_from_dict(credentials_dict)
    else:
        raise ValueError(
            f"{gspread_credentials_key} is not set or is invalid in the .env file."
        )

    sh = gc.open(f"{year} Fantasy Box Office Draft")

    worksheet_title = "Dashboard"
    worksheet = sh.worksheet(worksheet_title)

    sh.del_worksheet(worksheet)
    # 3 rows for title, 1 row for column titles, 1 row for footer
    sheet_height = len(released_movies_df) + 5
    worksheet = sh.add_worksheet(
        title=worksheet_title, rows=sheet_height, cols=25, index=1
    )

    # Adding each dashboard element
    for element in dashboard_elements:
        df_to_sheet(
            df=element[0],
            worksheet=worksheet,
            location=element[1],
            format_dict=element[2] if len(element) > 2 else None,
        )
        time.sleep(60)  # I need to figure out rate limiting

    # Adding last updated header
    worksheet.update(
        values=[
            [
                f'Last Updated UTC\n{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
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
        "I4:X4",
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
    spacer_columns = ["A", "H", "Y"]
    for column in spacer_columns:
        gsf.set_column_width(worksheet, column, 25)

    time.sleep(60)  # I need to figure out rate limiting

    # for some reason the auto resize still cuts off some of the title
    title_columns = ["J", "U"]

    if add_worst_picks:
        title_columns.append("C")

    for column in title_columns:
        gsf.set_column_width(worksheet, column, 284)

    # revenue columns will also get cut off
    revenue_columns = ["L", "M", "R", "S"]
    for column in revenue_columns:
        gsf.set_column_width(worksheet, column, 120)

    # gets resized wrong and have to do it manually
    gsf.set_column_width(worksheet, "R", 142)
    gsf.set_column_width(worksheet, "W", 104)
    gsf.set_column_width(worksheet, "X", 106)

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
    worksheet.merge_cells("I2:X2")

    if add_worst_picks:
        worksheet.update(values=[["Worst Picks"]], range_name="B11")
        worksheet.format(
            "B11",
            {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
        )
        worksheet.merge_cells("B11:G11")

    still_in_theater_rule = gsf.ConditionalFormatRule(
        ranges=[gsf.GridRange.from_a1_range("X5:X", worksheet)],
        booleanRule=gsf.BooleanRule(
            condition=gsf.BooleanCondition("TEXT_EQ", ["Yes"]),
            format=gsf.CellFormat(
                backgroundColor=gsf.Color(0, 0.9, 0),
            ),
        ),
    )

    rules = gsf.get_conditional_format_rules(worksheet)
    rules.append(still_in_theater_rule)
    rules.save()

    logger.info("Dashboard updated and formatted")

    draft_df = pd.read_csv(f"assets/drafts/{year}/box_office_draft.csv")
    released_movies = released_movies_df["Title"].tolist()
    drafted_movies = draft_df["movie"].tolist()
    movies_missing_from_scoreboard = list(set(drafted_movies) - set(released_movies))

    if movies_missing_from_scoreboard:
        logger.info("Movies missing from scoreboard:")
        logger.info(", ".join(sorted(movies_missing_from_scoreboard)))
    else:
        logger.info("All movies are on the scoreboard.")
