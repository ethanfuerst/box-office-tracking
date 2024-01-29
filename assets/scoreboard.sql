with full_table as (
    select
        *
        , case when round > 13 then 5 else 1 end as multiplier
        , multiplier * revenue as scored_revenue
        , revenue / budget as roi
        , release_date <= today() and release_date != '' as released
    from read_json_auto('<<daily_score>>')
)

select
    name
    , sum(scored_revenue) as scored_revenue
    , sum(released::int)::int as num_released
    , coalesce(sum(case when released then budget end), 0) as total_budget
    , round(coalesce((sum(revenue) / total_budget) - 1, 0) * 100, 2) / 100 as avg_roi
from full_table
group by 1
order by 2 desc, 3;