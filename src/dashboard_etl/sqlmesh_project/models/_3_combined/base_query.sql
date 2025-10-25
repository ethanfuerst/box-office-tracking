MODEL (
  name combined.base_query,
  kind FULL
);

select
    row_number() over (
        order by
            base_query_int.scored_revenue desc
            , base_query_int.title asc
    ) as rank
    , base_query_int.title
    , base_query_int.drafted_by
    , base_query_int.revenue
    , base_query_int.scored_revenue
    , base_query_int.round
    , base_query_int.overall_pick
    , base_query_int.multiplier
    , base_query_int.domestic_rev
    , base_query_int.domestic_pct
    , base_query_int.foreign_rev
    , base_query_int.foreign_pct
    , coalesce(better_pick_final.better_pick_title, '') as better_pick_title
    , coalesce(
        better_pick_final.better_pick_scored_revenue
        , 0
    ) as better_pick_scored_revenue
    , strftime(
        base_query_int.first_seen_date
        , '%m/%d/%Y'
    ) as first_seen_date
    , case
        when
            base_query_int.still_in_theaters
            or datediff(
                'day'
                , base_query_int.first_seen_date
                , today()
            ) <= 7
            then 'Yes'
        else 'No'
    end as still_in_theaters
from combined.base_query_int as base_query_int
left join combined.better_pick_final as better_pick_final
    on base_query_int.overall_pick = better_pick_final.overall_pick
where base_query_int.drafted_by is not null
order by 1 asc
