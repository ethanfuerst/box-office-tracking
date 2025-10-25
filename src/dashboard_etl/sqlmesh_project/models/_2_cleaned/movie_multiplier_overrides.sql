MODEL (
  name cleaned.movie_multiplier_overrides,
  kind FULL
);

select
    value as movie
    , multiplier
from raw.multipliers_and_exclusions
where type = 'movie'
