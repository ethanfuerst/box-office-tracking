MODEL (
  name raw.drafter,
  kind FULL
);

-- External table referenced:
-- @EXTERNAL drafter

select
    movie
    , name
    , overall_pick
    , round
from raw_drafter
