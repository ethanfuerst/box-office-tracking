MODEL (
  name raw.worldwide_snapshot,
  kind FULL
);

select
    *
from read_parquet('s3://' || @bucket || '/raw/worldwide_snapshot/release_group_id=*/scraped_date=*/data.parquet', filename=true, union_by_name=true)
