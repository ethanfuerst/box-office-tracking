create table base_query as (
    with parsed_data as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , loaded_date
            , min(loaded_date) over (partition by title) as first_seen_date
        from s3_dump
        qualify row_number() over (partition by title, year order by loaded_date desc) = 1
    )

    , currently_updating as (
        select
            title
            , lead(revenue, 1) over (partition by title order by loaded_date desc) as last_day_revenue
            , lag(revenue, 7) over (partition by title order by loaded_date desc) as revenue_7_days_ago
            , revenue != last_day_revenue as change_in_last_day
            , revenue != revenue_7_days_ago as change_in_last_week
            , loaded_date as last_updated_date
            , datediff('day', last_updated_date, today()) as days_since_last_update
            , coalesce((change_in_last_day or change_in_last_week) and days_since_last_update <= 7, false) as still_in_theaters
        from s3_dump
        qualify row_number() over (partition by title order by loaded_date desc) = 1
    )

    , manual_adds as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , release_date as first_seen_date
        from read_csv_auto('assets/manual_adds.csv')
    )

    , with_manual_adds as (
        select
            title
            , sum(revenue) as revenue
            , sum(domestic_rev) as domestic_rev
            , sum(foreign_rev) as foreign_rev
            , min(first_seen_date) as first_seen_date
        from parsed_data
        group by
            1
        
        union all
        
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , first_seen_date
        from manual_adds
    )

    , base_table as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , first_seen_date
        from with_manual_adds
        qualify row_number() over (partition by title order by revenue desc) = 1
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
            , base_table.first_seen_date
            , case when base_table.title in (select title from manual_adds) then false else coalesce(currently_updating.still_in_theaters, false) end as still_in_theaters
        from base_table
        inner join drafter
            on base_table.title = drafter.movie
        left join currently_updating
            on base_table.title = currently_updating.title
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
        , strftime(full_data.first_seen_date, '%m/%d/%Y') as first_seen_date
        , case when full_data.still_in_theaters then 'Yes' else 'No' end as still_in_theaters
    from full_data
    left join better_pick_final
        on full_data.overall_pick = better_pick_final.overall_pick
    order by
        rank asc
)