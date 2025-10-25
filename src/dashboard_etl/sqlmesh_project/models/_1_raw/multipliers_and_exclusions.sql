MODEL (
  name raw.multipliers_and_exclusions,
  kind FULL
);

-- This table is loaded from Google Sheets in the load_override_tables function
-- It contains multiplier overrides and exclusions data

select
    try_cast(value as varchar) as value
    , try_cast(multiplier as double) as multiplier
    , try_cast(type as varchar) as type
from raw_multipliers_and_exclusions
