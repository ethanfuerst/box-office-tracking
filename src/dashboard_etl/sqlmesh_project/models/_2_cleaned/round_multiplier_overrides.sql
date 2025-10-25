MODEL (
  name cleaned.round_multiplier_overrides,
  kind FULL
);

select
    try_cast(value as varchar) as round
    , try_cast(multiplier as double) as multiplier
from raw.multipliers_and_exclusions
where try_cast(type as varchar) = 'round'
