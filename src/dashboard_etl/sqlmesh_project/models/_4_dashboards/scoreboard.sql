MODEL (
  name dashboards.scoreboard,
  kind FULL
);

select
    drafted_by as drafted_by_name
    , sum(scored_revenue) as scored_revenue
    , count(distinct title)::int as num_released
    , sum(
        case
            when better_pick_scored_revenue = 0 then 1
            else 0
        end
    ) as correctly_drafted_pick_count
    , coalesce(
        round(correctly_drafted_pick_count / num_released, 4), 0
    ) as correct_pick_pct
    , sum(revenue) as unadjusted_revenue
from combined.base_query
where drafted_by is not null
group by
    1
order by
    2 desc
    , 3
