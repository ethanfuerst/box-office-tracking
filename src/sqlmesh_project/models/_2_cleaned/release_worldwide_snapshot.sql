MODEL (
  name cleaned.release_worldwide_snapshot,
  kind FULL
);

select
    movie_title
    , release_group_id
    , region
    , market
    , release_date
    , try_cast(opening as int) as opening
    , try_cast(total_gross as int) as total_gross
    , release_group_url
    , scraped_date
from raw.release_worldwide_snapshot
