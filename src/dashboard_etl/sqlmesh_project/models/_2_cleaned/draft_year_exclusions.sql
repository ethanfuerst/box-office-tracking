MODEL (
  name cleaned.draft_year_exclusions,
  kind FULL
);

select
    try_cast(value as varchar) as movie
from raw.multipliers_and_exclusions
where try_cast(type as varchar) = 'exclusion'
