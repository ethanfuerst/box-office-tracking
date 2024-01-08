import os
import pandas as pd
import datetime
import modal
import duckdb
import json

stub = modal.Stub("box-office-tracking")

if modal.is_local():
    draft = pd.read_csv("data/box_office_draft.csv").to_json(orient="records")
    global_vars = {"draft": json.loads(draft)}
    stub.data_dict = modal.Dict.new(global_vars)

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install requests", "pip install duckdb==0.9.2", "pip install pandas==2.1.4"
)

table_name = f"movie_records_{datetime.date.today().strftime('%Y%m%d')}"
file_name = f"{table_name}.json"
s3_file = f"s3://box-office-tracking/{table_name}.parquet"

movie_id_regex = r".*movie\/(\d+)-.*"


def get_movie_data(id):
    if id is np.nan:
        return None

    url = f"https://api.themoviedb.org/3/movie/{id}?api_key={MOVIE_DB_API_KEY}"

    response = requests.get(url)

    # error handling?

    data = response.json()
    return {
        "revenue": data["revenue"],
        "imdb_id": data["imdb_id"],
        "budget": data["budget"],
        "title": data["title"],
        "status": data["status"],
        "release_date": data["release_date"],
        "runtime": data["runtime"],
        "vote_average": data["vote_average"],
        "vote_count": data["vote_count"],
        "popularity": data["popularity"],
    }


def create_data():
    draft_data = stub.data_dict["draft"]

    df = pd.read_json(draft_data, orient="records")

    df["movie_id"] = df["url"].str.extract(movie_id_regex)
    df["stats"] = df["movie_id"].apply(get_movie_data)
    df = df.join(pd.json_normalize(df["stats"])).drop("stats", axis="columns")

    df["title"] = df["title"].fillna(df["movie"])
    df = df.drop("movie", axis="columns")
    df["record_date"] = str(datetime.date.today())

    df.to_json(file_name, orient="records")

    return


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 3 * * *"),
    secret=modal.Secret.from_name("box-office-tracking-secrets"),
    retries=5,
)
def record_movies():
    MOVIE_DB_API_KEY = os.environ["MOVIE_DB_API_KEY"]

    create_data()

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
        f"create or replace table {table_name} as select * "
        f"from read_json_auto('{file_name}');"
    )

    duckdb_con.execute(f"copy {table_name} to '{s3_file}'; drop table {table_name};")
    duckdb_con.close()

    os.remove(file_name)


if __name__ == "__main__":
    modal.runner.deploy_stub(stub)