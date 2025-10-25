MODEL (
  name box_office_tracking.drafter,
  kind FULL
);

-- External table referenced:
-- @EXTERNAL drafter

select
    movie
    , name
    , overall_pick
    , round
from drafter
