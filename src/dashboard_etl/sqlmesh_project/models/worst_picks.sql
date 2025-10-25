MODEL (
  name box_office_tracking.worst_picks,
  kind FULL
);

with better_picks as (
    select
        title
        , count(distinct better_pick_title) as number_of_better_picks
        , max(missed_revenue) as max_better_pick_revenue
    from box_office_tracking.better_pick_int
    group by 1
)

, final as (
    select
        row_number()
            over (
                order by
                    better_picks.number_of_better_picks desc
                    , base_query.overall_pick asc
            )
            as rank
        , base_query.title
        , base_query.drafted_by
        , base_query.overall_pick
        , better_picks.number_of_better_picks
        , better_picks.max_better_pick_revenue as missed_revenue
    from box_office_tracking.base_query as base_query
    inner join better_picks
        on base_query.title = better_picks.title
    where better_picks.number_of_better_picks > 0
)

select
    row_number() over (
        order by
            missed_revenue desc
            , number_of_better_picks desc
    ) as rank
    , title
    , drafted_by
    , overall_pick
    , number_of_better_picks
    , missed_revenue
from final
order by 1 asc
