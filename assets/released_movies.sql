with full_table as (
    select
        *
        , case when round > 13 then 5 else 1 end as multiplier
        , multiplier * revenue as scored_revenue
        , round(((revenue / budget) - 1) * 100, 2) / 100 as roi
        , release_date <= today() and release_date != '' as released
    from read_json_auto('<<daily_score>>')
    where released
)

select
    row_number() over (order by scored_revenue desc) as rank
    , title
    , name as drafted_by
    , strftime(release_date::date, '%-d/%-m/%Y') as release_date
    , today() - release_date::date as days_since_released
    , revenue
    , scored_revenue
    , budget
    , coalesce(roi, 0) as roi
    , popularity
    , runtime
from full_table
order by rank asc;