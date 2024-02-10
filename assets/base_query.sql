with base_table as (
    select
        "Release Group" as title
        , replace("Worldwide"[2:], ',', '')::int as revenue
        , coalesce(nullif(replace("Domestic"[2:], ',', ''), ''), 0)::int as domestic_rev
        , coalesce(nullif(replace("Foreign"[2:], ',', ''), ''), 0)::int as foreign_rev
    from read_json('s3_dump.json', auto_detect=true, format='auto')
    qualify row_number() over (partition by title order by revenue desc) = 1
)

, drafter as (
    select
        movie
        , name
        , round
    from read_csv('assets/box_office_draft.csv', auto_detect=true)
)

, joined as (
    select
        base_table.title
        , drafter.name as drafted_by
        , base_table.revenue
        , drafter.round
        , case when drafter.round > 13 then 5 else 1 end as multiplier
        , multiplier * base_table.revenue as scored_revenue
        , base_table.domestic_rev
        , base_table.foreign_rev
        , round(base_table.domestic_rev / base_table.revenue, 4) as domestic_pct
        , round(base_table.foreign_rev / base_table.revenue, 4) as foreign_pct
    from base_table
    inner join drafter
        on base_table.title = drafter.movie
)

select
    row_number() over (order by scored_revenue desc) as rank
    , title
    , drafted_by
    , revenue
    , scored_revenue
    , round
    , multiplier
    , domestic_rev
    , domestic_pct
    , foreign_rev
    , foreign_pct
from joined
order by rank asc