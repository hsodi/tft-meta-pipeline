with trait_data as (
    select * from {{ ref('int_trait_performance') }}
)

select
    trait_name,
    tier_current,
    games_played,
    top4_rate,
    win_rate,
    avg_placement,
    avg_units_active,
    sufficient_sample,

    -- rank traits by top4 rate within each tier
    rank() over (
        partition by tier_current
        order by case when sufficient_sample then top4_rate else null end desc
    ) as top4_rank_in_tier,

    -- rank across all traits regardless of tier
    rank() over (
        order by case when sufficient_sample then top4_rate else null end desc
    ) as overall_top4_rank

from trait_data
order by top4_rate desc