MODEL (
  name combined.base_query_int,
  kind FULL
);

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
    from cleaned.box_office_mojo_dump
    where title not in (
        select distinct movie
        from cleaned.draft_year_exclusions
    )
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
    where year_part == @year
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
    from cleaned.box_office_mojo_dump
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
            select distinct title
            from cleaned.manual_adds
        )
    group by 1

    union all

    select
        title
        , revenue
        , domestic_rev
        , foreign_rev
        , first_seen_date
    from cleaned.manual_adds
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
        select distinct title
        from cleaned.manual_adds
    ) then false
        else coalesce(
            currently_updating.still_in_theaters
            , false
        )
    end as still_in_theaters
from base_table
left join cleaned.drafter as drafter
    on base_table.title = drafter.movie
left join currently_updating
    on base_table.title = currently_updating.title
left join cleaned.round_multiplier_overrides as round_multiplier_overrides
    on drafter.round = round_multiplier_overrides.round
left join cleaned.movie_multiplier_overrides as movie_multiplier_overrides
    on base_table.title = movie_multiplier_overrides.movie
