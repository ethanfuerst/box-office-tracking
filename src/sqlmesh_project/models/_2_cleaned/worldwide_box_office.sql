MODEL (
  name cleaned.worldwide_box_office,
  kind FULL
);

select
    "Release Group" as title
    , coalesce(try_cast(replace(substring("Worldwide", 2), ',', '') as integer), 0) as revenue
    , coalesce(try_cast(replace(substring("Domestic", 2), ',', '') as integer), 0) as domestic_rev
    , coalesce(try_cast(replace(substring("Foreign", 2), ',', '') as integer), 0) as foreign_rev
    , cast(scraped_date_from_s3 as date) as loaded_date
    , cast(release_year as int) as release_year
from raw.worldwide_box_office
