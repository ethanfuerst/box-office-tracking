from logging import getLogger

from utils.db_connection import DuckDBConnection

S3_DATE_FORMAT = "%Y%m%d"


logger = getLogger(__name__)


def extract() -> None:
    duckdb_con = DuckDBConnection()

    duckdb_con.execute(
        f"""
        create temp table s3_dump as (
            with all_data as (
                select
                    *
                    , split_part(split_part(filename, '_', -1), '.', -2) as date_str
                    , split_part(filename, '_', -2) as year_part
                from read_parquet('s3://box-office-tracking/boxofficemojo_*', filename=true)
            )
            
            select
                "Release Group" as title
                , replace("Worldwide"[2:], ',', '')::int as revenue
                , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                , strptime(date_str, '{S3_DATE_FORMAT}') as loaded_date
                , date_part('year', loaded_date) as year
                , if(year_part = 'ytd', try_cast(year as int), try_cast(year_part as int)) as year_part
            from all_data
        )
        """
    )

    row_count = "select count(*) from s3_dump"
    logger.info(
        f'Read {duckdb_con.sql(row_count).fetchnumpy()["count_star()"][0]} rows with query from s3 bucket'
    )


if __name__ == "__main__":
    extract()
