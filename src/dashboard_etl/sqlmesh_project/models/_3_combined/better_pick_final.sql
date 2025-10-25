MODEL (
  name combined.better_pick_final,
  kind FULL
);

select
    overall_pick
    , better_pick_title
    , better_pick_scored_revenue
from combined.better_pick_int as better_pick_int
where better_pick_title is not null and overall_pick is not null and not better_pick_drafted_by_someone_else
qualify row_number() over (
    partition by overall_pick
    order by better_pick_scored_revenue desc
) = 1
