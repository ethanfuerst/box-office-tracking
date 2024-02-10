select
    drafted_by as name
    , sum(scored_revenue) as scored_revenue
    , count(*)::int as num_released
    , sum(revenue) as unadjusted_revenue
from (<<base_query>>)
group by
    1
order by
    2 desc
    , 3