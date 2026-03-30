with traits_exploded as (
    select
        match_id,
        puuid,
        placement,
        is_top4,
        is_first,
        json_extract_scalar(trait, '$.name')            as trait_name,
        cast(json_extract_scalar(trait, '$.tier_current') 
            as integer)                                 as tier_current,
        cast(json_extract_scalar(trait, '$.num_units') 
            as integer)                                 as num_units
    from {{ ref('stg_matches') }},
    unnest(traits_array) as trait
),

-- only count traits that are actually active (tier > 0)
active_traits as (
    select *
    from traits_exploded
    where tier_current > 0
),

trait_stats as (
    select
        trait_name,
        tier_current,
        count(*)                                        as games_played,
        avg(placement)                                  as avg_placement,
        avg(case when is_top4 then 1.0 else 0.0 end)    as top4_rate,
        avg(case when is_first then 1.0 else 0.0 end)   as win_rate,
        avg(num_units)                                  as avg_units_active
    from active_traits
    group by trait_name, tier_current
)

select
    trait_name,
    tier_current,
    games_played,
    round(top4_rate, 4)                                 as top4_rate,
    round(win_rate, 4)                                  as win_rate,
    round(avg_placement, 3)                             as avg_placement,
    round(avg_units_active, 2)                          as avg_units_active,
    case
        when games_played >= 30 then true
        else false
    end                                                 as sufficient_sample
from trait_stats
order by games_played desc