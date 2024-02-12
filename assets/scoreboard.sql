select
    drafted_by as name
    , sum(scored_revenue) as scored_revenue
    , count(distinct title)::int as num_released
    , sum((better_pick_scored_revenue == 0)::int) as correctly_drafted_pick_count
    , coalesce(round(correctly_drafted_pick_count / num_released, 4), 0) as correct_pick_pct
    , sum(revenue) as unadjusted_revenue
from (<<base_query>>)
group by
    1
order by
    2 desc
    , 3