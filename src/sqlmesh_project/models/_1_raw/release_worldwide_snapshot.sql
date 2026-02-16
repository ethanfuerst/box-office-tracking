MODEL (
  name raw.release_worldwide_snapshot,
  kind FULL
);

select
    *
from read_parquet('s3://' || @bucket || '/raw/release_worldwide_snapshot/release_group_id=*/scraped_date=*/data.parquet', filename=true, union_by_name=true)
