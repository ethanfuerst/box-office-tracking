create or replace table drafter as (
    select
        round
        , overall as overall_pick
        , name
        , movie
    from
        read_csv('assets/drafts/$year/box_office_draft.csv', auto_detect = true)
);

create or replace table round_multiplier_overrides as (
    select
        round
        , multiplier
    from
        read_csv(
            'assets/drafts/$year/round_multiplier_overrides.csv'
            , auto_detect = true
        )
);

create or replace table movie_multiplier_overrides as (
    select
        movie
        , multiplier
    from
        read_csv(
            'assets/drafts/$year/movie_multiplier_overrides.csv'
            , auto_detect = true
        )
);

create or replace table movie_better_pick_exclusions as (
    select movie
    from
        read_csv(
            'assets/drafts/$year/movie_better_pick_exclusions.csv'
            , auto_detect = true
        )
);

create or replace table manual_adds as (
    select
        title
        , try_cast(revenue as integer) as revenue
        , try_cast(domestic_rev as integer) as domestic_rev
        , try_cast(foreign_rev as integer) as foreign_rev
        , try_cast(release_date as date) as first_seen_date
    from read_csv_auto('assets/drafts/$year/manual_adds.csv')
);
