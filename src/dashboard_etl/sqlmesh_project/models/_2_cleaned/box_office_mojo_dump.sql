MODEL (
  name cleaned.box_office_mojo_dump,
  kind FULL
);

select
    title
    , revenue
    , domestic_rev
    , foreign_rev
    , loaded_date
    , year_part
from raw.box_office_mojo_dump
