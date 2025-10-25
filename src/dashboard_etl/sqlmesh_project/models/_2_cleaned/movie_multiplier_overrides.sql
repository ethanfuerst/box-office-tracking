MODEL (
  name cleaned.movie_multiplier_overrides,
  kind FULL
);

select
    movie
    , multiplier
from raw.movie_multiplier_overrides
