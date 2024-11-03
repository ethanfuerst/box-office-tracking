import datetime
import duckdb
import os
import ssl
import sys
import logging
import pandas as pd

from utils.db_connection import DuckDBConnection

S3_DATE_FORMAT = "%Y%m%d"


ssl._create_default_https_context = ssl._create_unverified_context
logger = logging.getLogger(__name__)


def get_most_recent_s3_date(duckdb_con: DuckDBConnection) -> datetime.date:
    max_date = duckdb_con.sql(
        f"""select
            max(make_date(file[44:47]::int, file[48:49]::int, file[50:51]::int)) as max_date
        from glob('s3://box-office-tracking/boxofficemojo_ytd_*');"""
    )
    return_val = max_date.fetchnumpy()["max_date"][0].astype(datetime.date).date()
    return return_val


def load_current_worldwide_box_office_to_s3(duckdb_con: DuckDBConnection) -> None:
    logger.info("Starting extraction.")
    try:
        df = pd.read_html("https://www.boxofficemojo.com/year/world/")[0]
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return

    box_office_data_table_name = (
        f"boxofficemojo_ytd_{datetime.datetime.today().strftime(S3_DATE_FORMAT)}"
    )
    box_office_data_file_name = f"{box_office_data_table_name}.json"
    s3_file = f"s3://box-office-tracking/{box_office_data_table_name}.parquet"

    with open(box_office_data_file_name, "w") as file:
        df.to_json(file, orient="records")

    duckdb_con.execute(
        f"copy (select * from read_json_auto('{box_office_data_table_name}.json')) to '{s3_file}';"
    )
    row_count = f"select count(*) from '{s3_file}';"
    logger.info(
        f"Updated {s3_file} with {duckdb_con.sql(row_count).fetchnumpy()['count_star()'][0]} rows."
    )
    os.remove(box_office_data_file_name)

    return


def extract() -> None:
    duckdb_con = DuckDBConnection()
    if get_most_recent_s3_date(duckdb_con) < datetime.date.today():
        logger.info("Loading new worldwide box office data to s3")
        load_current_worldwide_box_office_to_s3(duckdb_con)

    duckdb_con.execute(
        f"""
        create temp table s3_dump as (
            with all_data as (
                select
                    *
                from read_parquet('s3://box-office-tracking/boxofficemojo_ytd_*', filename=true)
            )
            
            , parsed_data as (
                select
                    "Release Group" as title
                    , replace("Worldwide"[2:], ',', '')::int as revenue
                    , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                    , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                    , strptime(filename[44:51], '{S3_DATE_FORMAT}') as loaded_date
                    , date_part('year', loaded_date) as year
                    , min(loaded_date) over (partition by title) as first_seen_date
                from all_data
                qualify row_number() over (partition by title, year order by loaded_date desc) = 1
            )
            
            , final as (
                select
                    title
                    , sum(revenue) as revenue
                    , sum(domestic_rev) as domestic_rev
                    , sum(foreign_rev) as foreign_rev
                    , min(first_seen_date) as first_seen_date
                from parsed_data
                group by
                    1
                
                union all
                
                select
                    title
                    , revenue
                    , domestic_rev
                    , foreign_rev
                    , release_date as first_seen_date
                from read_csv_auto('assets/manual_adds.csv')
            )
            
            select
                title
                , revenue
                , domestic_rev
                , foreign_rev
                , first_seen_date
            from final
            qualify row_number() over (partition by title order by revenue desc) = 1
        )"""
    )
    row_count = "select count(*) from s3_dump"
    logger.info(
        f"Read {duckdb_con.sql(row_count).fetchnumpy()['count_star()'][0]} rows with query from s3 bucket"
    )


if __name__ == "__main__":
    extract()
