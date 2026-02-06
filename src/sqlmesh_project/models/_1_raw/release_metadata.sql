MODEL (
  name raw.release_metadata,
  kind FULL
);

select
    *
from read_parquet('s3://' || @bucket || '/raw/release_metadata/release_id=*/scraped_date=*/data.parquet', filename=true, union_by_name=true)
