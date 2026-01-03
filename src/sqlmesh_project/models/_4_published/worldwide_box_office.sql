MODEL (
  name published.worldwide_box_office,
  kind FULL
);

select
    title
    , revenue
    , domestic_rev
    , foreign_rev
    , loaded_date
    , release_year
    , timezone('UTC', now())::timestamp as published_timestamp_utc
from cleaned.worldwide_box_office
