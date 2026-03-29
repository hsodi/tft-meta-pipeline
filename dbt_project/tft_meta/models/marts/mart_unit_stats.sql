with unit_data as (
    select * from {{ ref('int_unit_performance') }}
)

select
    unit_name,
    games_played,
    top4_rate,
    win_rate,
    avg_placement,
    avg_star_level,
    sufficient_sample,

    -- Rank units by top4 rate among those with sufficient sample
    rank() over (
        order by case when sufficient_sample then top4_rate else null end desc
    ) as top4_rank,

    -- Rank units by win rate
    rank() over (
        order by case when sufficient_sample then win_rate else null end desc
    ) as win_rank

from unit_data
order by top4_rate desc