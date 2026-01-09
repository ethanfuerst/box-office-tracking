MODEL (
  name raw.worldwide_box_office,
  kind FULL
);

select
    *
    , split_part(split_part(filename, 'release_year=', 2), '/', 1) as release_year
    , strptime(split_part(split_part(filename, 'scraped_date=', 2), '/', 1), '%Y-%m-%d') as scraped_date_from_s3
from read_parquet('s3://' || @bucket || '/raw/worldwide_box_office/release_year=*/scraped_date=*/data.parquet', filename=true)
