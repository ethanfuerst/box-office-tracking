MODEL (
  name raw.release_id_lookup,
  kind FULL
);

select
    *
from read_parquet('s3://' || @bucket || '/raw/release_id_lookup/release_year=*/scraped_date=*/data.parquet', filename=true)
