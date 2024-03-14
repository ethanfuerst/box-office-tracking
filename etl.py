import datetime
import duckdb
import glob
import gspread
import gspread_formatting as gsf
import html5lib
import json
import lxml
import modal
import os
import pandas as pd
import ssl
import sys
from pytz import timezone
from typing import List

stub = modal.Stub("box-office-tracking")

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install duckdb==0.9.2",
    "pip install gspread-formatting==1.1.2",
    "pip install gspread==6.0.2",
    "pip install html5lib==1.1",
    "pip install lxml==5.1.0",
    "pip install pandas==2.1.4",
    "pip install requests",
)

ssl._create_default_https_context = ssl._create_unverified_context

S3_DATE_FORMAT = "%Y%m%d"


def load_current_worldwide_box_office_to_s3() -> None:
    df = pd.read_html("https://www.boxofficemojo.com/year/world/")[0]

    box_office_data_table_name = (
        f"boxofficemojo_ytd_{datetime.datetime.today().strftime(S3_DATE_FORMAT)}"
    )
    box_office_data_file_name = f"{box_office_data_table_name}.json"
    s3_file = f"s3://box-office-tracking/{box_office_data_table_name}.parquet"

    df.to_json(box_office_data_file_name, orient="records")

    duckdb_con = duckdb.connect()
    duckdb_con.execute(
        f"""install httpfs;
        load httpfs;
        set s3_endpoint='nyc3.digitaloceanspaces.com';
        set s3_region='nyc3';
        set s3_access_key_id='{os.environ["S3_ACCESS_KEY_ID"]}';
        set s3_secret_access_key='{os.environ["S3_SECRET_ACCESS_KEY"]}';"""
    )
    duckdb_con.execute(
        f"copy (select * from read_json_auto('{box_office_data_table_name}.json')) to '{s3_file}';"
    )
    row_count = f"select count(*) from '{s3_file}';"
    print(
        f"Updated {s3_file} with {duckdb_con.sql(row_count).fetchnumpy()['count_star()'][0]} rows."
    )
    duckdb_con.close()
    os.remove(box_office_data_file_name)

    return


def df_to_sheet(df, worksheet, location, format_dict=None) -> None:
    worksheet.update(
        range_name=location, values=[df.columns.values.tolist()] + df.values.tolist()
    )
    print(f"Updated {location} with {df.shape[0]} rows and {df.shape[1]} columns")

    if format_dict:
        for format_location, format_rules in format_dict.items():
            worksheet.format(ranges=format_location, format=format_rules)
    return


def query_to_str(query_location: str) -> str:
    with open(query_location, "r") as file:
        query = file.read().replace("\n", " ").replace("\t", " ")

    return query


def temp_table_to_df(table, db_con, columns=None) -> pd.DataFrame:
    df = db_con.query(f"select * from {table}").df()
    if columns:
        df.columns = columns

    return df


def get_most_recent_s3_date(db_con) -> datetime.date:
    db_con.execute(
        f"""install httpfs;
        load httpfs;
        set s3_endpoint='nyc3.digitaloceanspaces.com';
        set s3_region='nyc3';
        set s3_access_key_id='{os.environ["S3_ACCESS_KEY_ID"]}';
        set s3_secret_access_key='{os.environ["S3_SECRET_ACCESS_KEY"]}';"""
    )
    max_date = db_con.sql(
        f"""select
            max(make_date(file[44:47]::int, file[48:49]::int, file[50:51]::int)) as max_date
        from glob('s3://box-office-tracking/boxofficemojo_ytd_*');"""
    )
    return_val = max_date.fetchnumpy()["max_date"][0].astype(datetime.date).date()
    return return_val


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secrets=[modal.Secret.from_name("box-office-tracking-secrets")],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[
        modal.Mount.from_local_dir("assets/", remote_path="/root/assets"),
        modal.Mount.from_local_dir("config/", remote_path="/root/config"),
    ],
)
def record_movies():
    duckdb_con = duckdb.connect()
    if get_most_recent_s3_date(duckdb_con) < datetime.date.today():
        print("Loading new worldwide box office data to s3")
        load_current_worldwide_box_office_to_s3()

    duckdb_con.execute(
        f"""install httpfs;
        load httpfs;
        set s3_endpoint='nyc3.digitaloceanspaces.com';
        set s3_region='nyc3';
        set s3_access_key_id='{os.environ["S3_ACCESS_KEY_ID"]}';
        set s3_secret_access_key='{os.environ["S3_SECRET_ACCESS_KEY"]}';
        
        create temp table s3_dump as (
            select
                "Release Group" as title
                , replace("Worldwide"[2:], ',', '')::int as revenue
                , coalesce(nullif(replace("Domestic"[2:], ',', ''), ''), 0)::int as domestic_rev
                , coalesce(nullif(replace("Foreign"[2:], ',', ''), ''), 0)::int as foreign_rev
            from read_parquet('s3://box-office-tracking/boxofficemojo_ytd_*')
            qualify row_number() over (partition by title order by revenue desc) = 1
        )"""
    )
    row_count = "select count(*) from s3_dump"
    print(
        f"Read {duckdb_con.sql(row_count).fetchnumpy()['count_star()'][0]} rows with query from s3 bucket"
    )

    for sql_file in sorted(glob.glob("assets/*.sql")):
        duckdb_con.execute(query_to_str(sql_file))

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
        ],
    )

    def remove_comments(obj):
        if isinstance(obj, dict):
            return {
                k: remove_comments(v)
                for k, v in obj.items()
                if not k.startswith("_comment")
            }
        elif isinstance(obj, list):
            return [remove_comments(item) for item in obj]
        else:
            return obj

    def load_format_config(file_path: str) -> dict:
        with open(file_path, "r") as file:
            config = json.load(file)
        return remove_comments(config)

    dashboard_elements = (
        (
            temp_table_to_df(
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
            ),
            "B4",
            load_format_config("assets/scoreboard_format.json"),
        ),
        (
            released_movies_df,
            "I4",
            load_format_config("assets/released_movies_format.json"),
        ),
    )
    duckdb_con.close()

    gc = gspread.service_account(
        filename="config/box-office-tracking-draft-e034f0e51fb4.json"
    )

    sh = gc.open("Fantasy Box Office")

    worksheet_title = "Dashboard"
    worksheet = sh.worksheet(worksheet_title)

    sh.del_worksheet(worksheet)
    # 3 rows for title, 1 row for column titles, 1 row for footer
    sheet_height = len(released_movies_df) + 5
    worksheet = sh.add_worksheet(
        title=worksheet_title, rows=sheet_height, cols=23, index=1
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
    worksheet.columns_auto_resize(8, 22)

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
    worksheet.format(
        "I4:V4",
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
    spacer_columns = ["A", "H", "W"]
    for column in spacer_columns:
        gsf.set_column_width(worksheet, column, 25)

    # for some reason the auto resize still cuts off some of the title
    title_columns = ["J", "U"]
    for column in title_columns:
        gsf.set_column_width(worksheet, column, 160)

    # revenue columns will also get cut off
    revenue_columns = ["L", "M", "R", "S"]
    for column in revenue_columns:
        gsf.set_column_width(worksheet, column, 120)

    # gets resized wrong and have to do it manually
    gsf.set_column_width(worksheet, "R", 142)

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
    worksheet.merge_cells("I2:V2")

    print("Dashboard updated and formatted")


if __name__ == "__main__":
    modal.runner.deploy_stub(stub)
