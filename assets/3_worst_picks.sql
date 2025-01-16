create or replace table worst_picks as (
    with base_query_cte as (
        select
            title
            , drafted_by
            , overall_pick
            , scored_revenue
            , multiplier
        from base_query
    )

    , better_picks as (
        select
            base.title
            , base.multiplier
            , count(distinct better_pick.title) as number_of_better_picks
            , max(better_pick.scored_revenue) as max_better_pick_revenue
        from base_query_cte as base
        left join base_query_cte as better_pick
            on
                base.scored_revenue < better_pick.scored_revenue
                and base.overall_pick < better_pick.overall_pick
                and base.drafted_by != better_pick.drafted_by
        where
            better_pick.scored_revenue > 0
        group by
            1, 2
    )

    , final as (
        select
            row_number()
                over (
                    order by
                        better_picks.number_of_better_picks desc
                        , base_query_cte.overall_pick asc
                )
                as rank
            , base_query_cte.title
            , base_query_cte.drafted_by
            , base_query_cte.overall_pick
            , better_picks.number_of_better_picks
            , better_picks.max_better_pick_revenue
            - base_query_cte.scored_revenue as missed_revenue
        from base_query_cte
        inner join better_picks
            on base_query_cte.title = better_picks.title
        where better_picks.number_of_better_picks > 0
    )

    select
        row_number() over (
            order by missed_revenue desc
        ) as rank
        , title
        , drafted_by
        , overall_pick
        , number_of_better_picks
        , missed_revenue
    from final
    order by 1 asc
)
