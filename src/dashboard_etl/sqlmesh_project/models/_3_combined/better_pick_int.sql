MODEL (
  name combined.better_pick_int,
  kind FULL
);

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
    , (picks.drafted_by != better_pick.drafted_by) or (better_pick.drafted_by is null) as better_pick_drafted_by_someone_else
from combined.base_query_int as picks
left join combined.base_query_int as better_pick
    on
        picks.scored_revenue < better_pick.scored_revenue
        and (
            (picks.overall_pick < better_pick.overall_pick)
            or (better_pick.overall_pick is null)
        )
left join cleaned.round_multiplier_overrides as round_multiplier_overrides
    on picks.round = round_multiplier_overrides.round
where (missed_revenue > 0 or missed_revenue is null)
