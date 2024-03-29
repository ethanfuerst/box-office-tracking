create table base_query as (
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

    , full_data as (
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
            , picks.multiplier
            , count(better_picks.overall_pick) > 0 as better_pick_available
            , case
                when better_pick_available and picks.multiplier = 1
                    then max(better_picks.revenue)
                when better_pick_available and picks.multiplier = 5
                    then max(better_picks.scored_revenue)
                else 0
            end as better_pick_scored_revenue
        from full_data as picks
        left join full_data as better_picks
            on picks.revenue < better_picks.revenue
            and picks.overall_pick < better_picks.overall_pick
        group by
            picks.round
            , picks.overall_pick
            , picks.revenue
            , picks.multiplier
    )

    , better_pick_final as (
        select
            better_pick_calc.overall_pick
            , better_picks_metadata.title as better_pick_title
            , better_pick_calc.better_pick_scored_revenue
        from better_pick_calc
        left join full_data as better_picks_metadata
            on (better_pick_calc.better_pick_scored_revenue = better_picks_metadata.scored_revenue
                or better_pick_calc.better_pick_scored_revenue = better_picks_metadata.revenue)
            and better_pick_calc.overall_pick < better_picks_metadata.overall_pick
    )

    select
        row_number() over (order by full_data.scored_revenue desc) as rank
        , full_data.title
        , full_data.drafted_by
        , full_data.revenue
        , full_data.scored_revenue
        , full_data.round
        , full_data.overall_pick
        , full_data.multiplier
        , full_data.domestic_rev
        , full_data.domestic_pct
        , full_data.foreign_rev
        , full_data.foreign_pct
        , coalesce(better_pick_final.better_pick_title, '') as better_pick_title
        , coalesce(better_pick_final.better_pick_scored_revenue, 0) as better_pick_scored_revenue
    from full_data
    left join better_pick_final
        on full_data.overall_pick = better_pick_final.overall_pick
    order by
        rank asc
)