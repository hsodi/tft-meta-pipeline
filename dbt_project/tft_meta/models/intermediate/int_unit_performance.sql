with units_exploded as (
    select
        match_id,
        puuid,
        placement,
        is_top4,
        is_first,
        json_extract_scalar(unit, '$.character_id')     as unit_name,
        cast(json_extract_scalar(unit, '$.tier') 
            as integer)                                 as tier,
    from {{ref('stg_matches')}},
    unnest(units_array) as unit
),

unit_stats as (
    select
        unit_name,
        count(*)                                        as games_played,
        avg(placement)                                  as avg_placement,
        avg(case when is_top4 then 1.0 else 0.0 end)        as top4_rate,
        avg(case when is_first then 1.0 else 0.0 end)       as win_rate,
        avg(tier)                                       as avg_star_level
    from units_exploded
    group by unit_name
)

select 
    unit_name,
    games_played,
    round(top4_rate, 4)                                 as top4_rate,
    round(win_rate, 4)                                  as win_rate,
    round(avg_placement, 3)                             as avg_placement,
    round(avg_star_level, 2)                            as avg_star_level,
    case 
        when games_played >= 30 then true
        else false
    end                                                 as sufficient_sample
from unit_stats
order by games_played desc