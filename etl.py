import os
import pandas as pd
import numpy as np
import datetime
import modal
import duckdb
import requests
import gspread_pandas as gp

stub = modal.Stub("box-office-tracking")

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install requests",
    "pip install duckdb==0.9.2",
    "pip install pandas==2.1.4",
    "pip install gspread_pandas==3.2.2",
)

table_name = f"movie_records_{datetime.date.today().strftime('%Y%m%d')}"
file_name = f"{table_name}.json"
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
    df = pd.read_csv("data/box_office_draft.csv")

    df["movie_id"] = df["url"].str.extract(movie_id_regex)
    df["stats"] = df["movie_id"].apply(lambda x: get_movie_data(x, api_key))
    df = df.join(pd.json_normalize(df["stats"])).drop("stats", axis="columns")

    df["title"] = df["title"].fillna(df["movie"])
    df = df.drop("movie", axis="columns")
    df["record_date"] = str(datetime.date.today())

    df.to_json(file_name, orient="records")

    return


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secret=modal.Secret.from_name("box-office-tracking-secrets"),
    retries=5,
    mounts=[
        modal.Mount.from_local_dir("data/", remote_path="/root/data"),
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
        f"copy (select * from read_json_auto('{file_name}')) to '{s3_file}';"
    )

    df = duckdb.query(
        f"""
        with full_table as (
            select
                *
                , case when round > 13 then 5 else 1 end as multiplier
                , multiplier * revenue as scored_revenue
                , revenue / budget as roi
                , revenue > 0 as released
            from (select * from read_json_auto('{file_name}'))
        )

        select
            name
            , sum(scored_revenue) as scored_revenue
            , sum(released::int)::int as num_released
            , coalesce(sum(case when released then budget end), 0) as total_budget
            , round(coalesce((sum(revenue) / total_budget) - 1, 0) * 100, 2) as avg_roi
        from full_table
        group by 1
        order by 2 desc, 3;
        """
    ).to_df()
    df.columns = ["Name", "Scored Revenue", "# Released", "Total Budget", "Avg ROI"]

    duckdb_con.close()

    spread = gp.Spread(
        "Fantasy Box Office",
        config=gp.conf.get_config(
            conf_dir="config/", file_name="box-office-tracking-draft-e034f0e51fb4.json"
        ),
    )

    spread.df_to_sheet(df, index=False, sheet="Dashboard", start="A1")
    spread.df_to_sheet(
        pd.DataFrame(
            columns=["Last Updated UTC"],
            data=[datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ),
        index=False,
        sheet="Dashboard",
        start="G1",
    )

    os.remove(file_name)


if __name__ == "__main__":
    modal.runner.deploy_stub(stub)

# %%
