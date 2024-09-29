WITH
athletes AS (
    SELECT *
    FROM {{ ref('athletes') }}
),

cast_types AS (
    SELECT
        -- CAST(id AS STRING) AS athlete_id,
        -- person_id,
        -- LOWER(person_displayname_fullname) AS person_displayname_fullname,
        TRIM(CONCAT(LOWER(person_fullname_firstname), ' ', LOWER(person_fullname_lastname))) AS person_fullname,
        audience,
        sport AS wrestling_type,
        maxWeight AS weight_class_in_kg,
        rank,
        uwwPoints AS uww_points,
        current AS is_current,
        previousRank AS previous_rank,
        previousSeasonRank AS previous_season_rank,
        season,
        CAST(dateRange_start AS DATE) AS daterange_start,
        CAST(dateRange_end AS DATE) AS daterange_end,
        LOWER(person_fullname_lastname) AS lastname,
        LOWER(person_fullname_firstname) AS firstname,
        -- person_shortname_lastname,
        -- person_shortname_firstname,
        person_noc AS nationality,
        person_gender,
        CAST(person_birthDate AS DATE) AS person_birthdate,
        person_profilePicture AS person_profilePicture
    FROM athletes
),

add_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
				'person_fullname',
                'weight_class_in_kg'
			])
		}} as athlete_key,
        *
    FROM cast_types
)

SELECT * FROM add_surrogate_key
