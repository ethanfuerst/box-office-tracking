MODEL (
  name cleaned.manual_adds,
  kind FULL
);

select
    title
    , revenue
    , domestic_rev
    , foreign_rev
    , first_seen_date
from raw.manual_adds
