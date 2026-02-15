MODEL (
  name raw.release_domestic,
  kind FULL
);

select
    *
from read_parquet('s3://' || @bucket || '/raw/release_domestic/release_id=*/scraped_date=*/data.parquet', filename=true, union_by_name=true)
