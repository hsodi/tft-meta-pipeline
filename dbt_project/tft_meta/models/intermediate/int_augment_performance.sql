with exploded as (
    select
        match_id,
        puuid,
        placement,
        is_top4,
        is_first,
        augment
    from {{ ref('stg_matches') }},
    unnest(augments_array) as augment
),

augment_stats as (
    select
        augment,
        count(*)                                        as games_played,
        sum(case when is_top4 then 1 else 0 end)        as top4_count,
        sum(case when is_first then 1 else 0 end)       as first_count,
        avg(placement)                                  as avg_placement,
        avg(case when is_top4 then 1.0 else 0.0 end)    as top4_rate,
        avg(case when is_first then 1.0 else 0.0 end)   as win_rate
    from exploded
    group by augment
)

select
    augment,
    games_played,
    top4_count,
    first_count,
    round(top4_rate, 4)                                 as top4_rate,
    round(win_rate, 4)                                  as win_rate,
    round(avg_placement, 3)                             as avg_placement,
    case 
        when games_played >= 5 then true 
        else false 
    end                                                 as sufficient_sample
from augment_stats
order by games_played desc