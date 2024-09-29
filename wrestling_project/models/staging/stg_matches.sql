WITH
matches AS (
    SELECT *
    FROM {{ ref('matches') }}
),

cast_types AS (
    SELECT
        TRIM(LOWER(wrestler_a)) AS wrestler_a,
        TRIM(LOWER(wrestler_b)) AS wrestler_b,
        score_a,
        score_b,
        match_result,
        -- image_a,
        -- image_b,
        tournament_name,
        -- tournament_weight_class,
        SUBSTR(tournament_weight_class, 0, 3) AS weight_class_in_kg,
        tournament_country,
        tournament_month_year,
        SUBSTR(tournament_month_year, -4) AS tournament_year
    FROM matches
),

add_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
				'wrestler_a', 
				'wrestler_b',
                'score_a',
                'score_b',
                'tournament_name',
                'tournament_month_year'
			])
		}} as match_key,
        {{ dbt_utils.generate_surrogate_key([
				'wrestler_a',
                'weight_class_in_kg'
			])
		}} as wrestler_a_key,
        {{ dbt_utils.generate_surrogate_key([
				'wrestler_b',
                'weight_class_in_kg'
			])
		}} as wrestler_b_key,
        {{ dbt_utils.generate_surrogate_key([
				'tournament_name',
                'tournament_year',
                'tournament_country'
			])
		}} as tournament_key,        
        *
    FROM cast_types    
)

SELECT * FROM add_surrogate_key

