MODEL (
  name cleaned.release_metadata,
  kind FULL
);

select
    release_id
    , movie_title
    , distributor
    , try_cast(opening_amount as int) as opening_amount
    , try_cast(opening_theaters as int) as opening_theaters
    , release_date
    , rating
    , runtime
    , genres
    , try_cast(widest_release as int) as widest_release
    , scraped_date
from raw.release_metadata
