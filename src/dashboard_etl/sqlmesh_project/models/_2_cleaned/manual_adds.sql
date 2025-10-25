MODEL (
  name cleaned.manual_adds,
  kind FULL
);

select
    try_cast(title as varchar) as title
    , try_cast(revenue as integer) as revenue
    , try_cast(domestic_rev as integer) as domestic_rev
    , try_cast(foreign_rev as integer) as foreign_rev
    , try_cast(release_date as date) as first_seen_date
from raw.manual_adds
