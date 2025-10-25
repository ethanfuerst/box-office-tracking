MODEL (
  name cleaned.drafter,
  kind FULL
);

select
    movie
    , name
    , overall_pick
    , round
from raw.drafter
