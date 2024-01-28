import os
import sys
import pandas as pd
import numpy as np
import datetime
import modal
import duckdb
import requests
import gspread
import gspread_formatting as gsf
import assets

stub = modal.Stub("box-office-tracking")

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install requests",
    "pip install duckdb==0.9.2",
    "pip install pandas==2.1.4",
    "pip install gspread==5.12.4",
    "pip install gspread-formatting==1.1.2",
)

table_name = f"movie_records_{datetime.date.today().strftime('%Y%m%d')}"
daily_file_name = "daily_score.json"
s3_file = f"s3://box-office-tracking/{table_name}.parquet"

movie_id_regex = r".*movie\/(\d+)-.*"


def get_movie_data(id, api_key):
    if id is np.nan:
        return None

    url = f"https://api.themoviedb.org/3/movie/{id}?api_key={api_key}"

    response = requests.get(url)

    if response.status_code != 200:
        print(f"Movie id {id} not found, skipping")
        return None

    data = response.json()

    return {
        key: response.json().get(key, None)
        for key in [
            "revenue",
            "imdb_id",
            "budget",
            "title",
            "status",
            "release_date",
            "runtime",
            "vote_average",
            "vote_count",
            "popularity",
        ]
    }


def create_data(api_key):
    df = pd.read_csv("assets/box_office_draft.csv")

    df["movie_id"] = df["url"].str.extract(movie_id_regex)
    df["stats"] = df["movie_id"].apply(lambda x: get_movie_data(x, api_key))
    df = df.join(pd.json_normalize(df["stats"])).drop("stats", axis="columns")

    df["title"] = df["title"].fillna(df["movie"])
    df = df.drop("movie", axis="columns")
    df["record_date"] = str(datetime.date.today())

    df.to_json(daily_file_name, orient="records")

    return


def df_to_sheet(df, worksheet, location, format=None):
    worksheet.update(
        range_name=location, values=[df.columns.values.tolist()] + df.values.tolist()
    )
    print(f"Updated {location} with {df.shape[0]} rows and {df.shape[1]} columns")

    if format:
        for format_location, format_rules in format.items():
            worksheet.format(ranges=format_location, format=format_rules)
    return


def query_to_df(query_location, source_tables=None, columns=None):
    with open(query_location, "r") as file:
        query = file.read().replace("\n", " ")

    if source_tables:
        for source_table_name, source_table_location in source_tables.items():
            query = query.replace(f"<<{source_table_name}>>", source_table_location)

    df = duckdb.query(query).to_df()
    if columns:
        df.columns = columns

    return df


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secret=modal.Secret.from_name("box-office-tracking-secrets"),
    retries=5,
    mounts=[
        modal.Mount.from_local_dir("assets/", remote_path="/root/assets"),
        modal.Mount.from_local_dir("config/", remote_path="/root/config"),
    ],
)
def record_movies():
    # add logging

    MOVIE_DB_API_KEY = os.environ["MOVIE_DB_API_KEY"]
    create_data(MOVIE_DB_API_KEY)

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
        f"copy (select * from read_json_auto('{daily_file_name}')) to '{s3_file}';"
    )

    dashboard_elements = (
        (
            query_to_df(
                "assets/scoreboard.sql",
                source_tables={
                    "daily_score": daily_file_name,
                },
                columns=[
                    "Name",
                    "Scored Revenue",
                    "# Released",
                    "Total Budget",
                    "Avg ROI",
                ],
            ),
            "B4",
            assets.get_scoreboard_format(),
        ),
    )

    gc = gspread.service_account(
        filename="config/box-office-tracking-draft-e034f0e51fb4.json"
    )

    sh = gc.open("Fantasy Box Office")

    worksheet_title = "Dashboard"
    worksheet = sh.worksheet(worksheet_title)

    sh.del_worksheet(worksheet)
    worksheet = sh.add_worksheet(title=worksheet_title, rows=10, cols=7, index=1)

    for element in dashboard_elements:
        df_to_sheet(
            df=element[0],
            worksheet=worksheet,
            location=element[1],
            format=element[2] if len(element) > 2 else None,
        )

    duckdb_con.close()
    os.remove(daily_file_name)

    worksheet.update("B2", "Fantasy Box Office Standings")
    worksheet.format(
        "B2",
        {"horizontalAlignment": "CENTER", "textFormat": {"fontSize": 20, "bold": True}},
    )
    worksheet.merge_cells("B2:E2")
    worksheet.update(
        "F2",
        f"Last Updated UTC\n{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    )
    worksheet.format(
        "F2",
        {
            "horizontalAlignment": "CENTER",
        },
    )

    column_sizes = {
        "A": 21,
        "B": 86,
        "C": 130,
        "D": 100,
        "E": 110,
        "F": 130,
        "G": 21,
    }
    for column, size in column_sizes.items():
        gsf.set_column_width(worksheet, column, size)


if __name__ == "__main__":
    modal.runner.deploy_stub(stub)
