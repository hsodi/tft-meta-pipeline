with source as (
    select * from `tft-meta-pipeline.tft_raw.raw_matches`
),

cleaned as (
    select
        match_id,
        timestamp_millis(game_datetime)     as game_at,
        tft_set,
        game_variation,
        puuid,
        placement,
        json_extract_array(augments)        as augments_array,
        json_extract_array(units)           as units_array,
        json_extract_array(traits)          as traits_array,
        total_damage_to_players,
        last_round,
        level,
        case 
            when placement <= 4 then true 
            else false 
        end                                 as is_top4,
        case 
            when placement = 1 then true 
            else false 
        end                                 as is_first,
        ingested_at,
        row_number() over (
            partition by match_id, puuid
            order by ingested_at desc
        )                                   as row_num
    from source
)

select * except(row_num)
from cleaned
where row_num = 1