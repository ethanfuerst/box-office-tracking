create temp table base_query as (
    with base_table as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
        from s3_dump
    )

    , drafter as (
        select
            round
            , overall as overall_pick
            , name
            , movie
        from read_csv('assets/box_office_draft.csv', auto_detect=true)
    )

    , joined as (
        select
            base_table.title
            , drafter.name as drafted_by
            , drafter.overall_pick
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

    , better_pick_calc as (
        select
            picks.round
            , picks.overall_pick
            , picks.revenue
            , count(better_picks.overall_pick) > 0 as better_pick_available
            , case
                when better_pick_available
                    then max(better_picks.scored_revenue)
                else 0
            end as better_pick_scored_revenue
        from joined as picks
        left join joined as better_picks
            on picks.scored_revenue < better_picks.scored_revenue
            and picks.overall_pick < better_picks.overall_pick
        group by
            picks.round
            , picks.overall_pick
            , picks.revenue
    )

    , better_pick_final as (
        select
            better_pick_calc.overall_pick
            , better_picks2.title as better_pick_title
            , better_pick_calc.better_pick_scored_revenue
        from better_pick_calc
        left join joined as better_picks2
            on better_pick_calc.better_pick_scored_revenue = better_picks2.scored_revenue
    )

    select
        row_number() over (order by joined.scored_revenue desc) as rank
        , joined.title
        , joined.drafted_by
        , joined.revenue
        , joined.scored_revenue
        , joined.round
        , joined.overall_pick
        , joined.multiplier
        , joined.domestic_rev
        , joined.domestic_pct
        , joined.foreign_rev
        , joined.foreign_pct
        , coalesce(better_pick_final.better_pick_title, '') as better_pick_title
        , coalesce(better_pick_final.better_pick_scored_revenue, 0) as better_pick_scored_revenue
    from joined
    left join better_pick_final
        on joined.overall_pick = better_pick_final.overall_pick
    order by
        rank asc
)