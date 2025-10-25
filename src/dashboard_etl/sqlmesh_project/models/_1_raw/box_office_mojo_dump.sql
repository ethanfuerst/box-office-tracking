MODEL (
  name raw.box_office_mojo_dump,
  kind FULL
);

-- External table referenced:
-- @EXTERNAL raw_box_office_mojo_dump

select
    title
    , revenue
    , domestic_rev
    , foreign_rev
    , loaded_date
    , year_part
from raw_box_office_mojo_dump
