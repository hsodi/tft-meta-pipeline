with augment_data as (
    select * from {{ ref('int_augment_performance') }}
),

-- Wilson score confidence interval for win rate
-- More accurate than normal approximation for extreme probabilities
final as (
    select
        augment,
        games_played,
        top4_rate,
        win_rate,
        avg_placement,
        sufficient_sample,

        -- Lower bound of 95% Wilson confidence interval
        round(
            (win_rate + 1.96 * 1.96 / (2 * games_played)
            - 1.96 * sqrt(
                win_rate * (1 - win_rate) / games_played
                + 1.96 * 1.96 / (4 * games_played * games_played)
            )) / (1 + 1.96 * 1.96 / games_played)
        , 4) as win_rate_ci_lower,

        -- Upper bound of 95% Wilson confidence interval
        round(
            (win_rate + 1.96 * 1.96 / (2 * games_played)
            + 1.96 * sqrt(
                win_rate * (1 - win_rate) / games_played
                + 1.96 * 1.96 / (4 * games_played * games_played)
            )) / (1 + 1.96 * 1.96 / games_played)
        , 4) as win_rate_ci_upper

    from augment_data
)

select * from final
order by win_rate desc