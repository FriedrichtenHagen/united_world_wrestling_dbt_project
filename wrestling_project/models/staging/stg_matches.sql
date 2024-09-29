WITH
matches AS (
    SELECT *
    FROM {{ ref('matches') }}
),

cast_types AS (
    SELECT
        LOWER(wrestler_a) AS wrestler_a,
        LOWER(wrestler_b) AS wrestler_b,
        score_a,
        score_b,
        match_result,
        -- image_a,
        -- image_b,
        tournament_name,
        tournament_weight_class,
        tournament_country,
        tournament_month_year
    FROM matches
)



SELECT * FROM cast_types