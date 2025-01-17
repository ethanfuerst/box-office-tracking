create or replace table base_query_int as ( -- noqa: LT05
    with raw_data as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , loaded_date
            , year_part
            , min(loaded_date) over (
                partition by title
            ) as first_seen_date
        from box_office_mojo_dump
    )

    , parsed_data as (
        select
            title
            , revenue
            , domestic_rev
            , foreign_rev
            , loaded_date
            , first_seen_date
        from raw_data
        where year_part == $year -- noqa: PRS, LXR
        qualify row_number() over (
                partition by title
                order by loaded_date desc
            ) = 1
    )

    , currently_updating as (
        select
            title
            , lead(revenue, 1) over (
                partition by title
                order by loaded_date desc
            ) as last_day_revenue
            , lag(revenue, 7) over (
                partition by title
                order by loaded_date desc
            ) as revenue_7_days_ago
            , revenue != last_day_revenue as change_in_last_day
            , revenue != revenue_7_days_ago as change_in_last_week
            , loaded_date as last_updated_date
            , datediff(
                'day'
                , last_updated_date
                , today()
            ) as days_since_last_update
            , coalesce(
                (change_in_last_day or change_in_last_week)
                and days_since_last_update <= 7
                , false
            ) as still_in_theaters
        from box_office_mojo_dump
        qualify row_number() over (
            partition by title
            order by loaded_date desc
        ) = 1
    )

    , with_manual_adds as (
        select
            parsed_data.title
            , sum(parsed_data.revenue) as revenue
            , sum(parsed_data.domestic_rev) as domestic_rev
            , sum(parsed_data.foreign_rev) as foreign_rev
            , min(parsed_data.first_seen_date) as first_seen_date
        from parsed_data
        where
            parsed_data.title not in (
                select distinct manual_adds.title
                from manual_adds
            )
        group by 1

        union all

        select
            manual_adds.title
            , manual_adds.revenue
            , manual_adds.domestic_rev
            , manual_adds.foreign_rev
            , manual_adds.first_seen_date
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
        qualify row_number() over (
            partition by title
            order by revenue desc
        ) = 1
    )

    select
        base_table.title
        , drafter.name as drafted_by
        , drafter.overall_pick
        , coalesce(base_table.revenue, 0) as revenue
        , drafter.round
        , coalesce(
            round_multiplier_overrides.multiplier::float
            , 1
        ) * coalesce(
            movie_multiplier_overrides.multiplier::float
            , 1
        ) as multiplier
        , round(
            coalesce(
                round_multiplier_overrides.multiplier::float
                , 1
            ) * coalesce(
                movie_multiplier_overrides.multiplier::float
                , 1
            ) * base_table.revenue::float
            , 0
        ) as scored_revenue
        , coalesce(base_table.domestic_rev, 0) as domestic_rev
        , coalesce(base_table.foreign_rev, 0) as foreign_rev
        , coalesce(
            round(
                base_table.domestic_rev / base_table.revenue
                , 4
            )
            , 0
        ) as domestic_pct
        , coalesce(
            round(
                base_table.foreign_rev / base_table.revenue
                , 4
            )
            , 0
        ) as foreign_pct
        , base_table.first_seen_date
        , case
            when base_table.title in (
                select distinct manual_adds.title
                from manual_adds
            ) then false
            else coalesce(
                currently_updating.still_in_theaters
                , false
            )
        end as still_in_theaters
    from base_table
    left join drafter
        on base_table.title = drafter.movie
    left join currently_updating
        on base_table.title = currently_updating.title
    left join round_multiplier_overrides
        on drafter.round = round_multiplier_overrides.round
    left join movie_multiplier_overrides
        on base_table.title = movie_multiplier_overrides.movie
);

create or replace table better_pick_int as (
    select
        picks.title
        , picks.overall_pick
        , better_pick.title as better_pick_title
        , picks.revenue as original_pick_revenue
        , round_multiplier_overrides.multiplier
        , coalesce(
            round_multiplier_overrides.multiplier
            , 1
        ) * better_pick.revenue as better_pick_scored_revenue
        , (
            coalesce(
                round_multiplier_overrides.multiplier
                , 1
            ) * better_pick.revenue
        ) - picks.scored_revenue as missed_revenue
    from base_query_int as picks
    left join base_query_int as better_pick
        on
            picks.scored_revenue < better_pick.scored_revenue
            and (
                (picks.overall_pick < better_pick.overall_pick)
                or (better_pick.overall_pick is null)
            )
            and (
                (picks.drafted_by != better_pick.drafted_by)
                or (better_pick.drafted_by is null)
            )
    left join round_multiplier_overrides
        on picks.round = round_multiplier_overrides.round
    where (missed_revenue > 0 or missed_revenue is null)
);

create or replace table better_pick_final as (
    select
        overall_pick
        , better_pick_title
        , better_pick_scored_revenue
    from better_pick_int
    qualify row_number() over (
        partition by overall_pick
        order by better_pick_scored_revenue desc
    ) = 1
);

create or replace table base_query as (
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
    from base_query_int
    left join better_pick_final
        on base_query_int.overall_pick = better_pick_final.overall_pick
    where base_query_int.drafted_by is not null
    order by 1 asc
);
