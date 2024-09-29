-- SELECT person_displayname_fullname, athlete_id, person_id, count(*) AS num FROM {{ ref("stg_athletes") }}
-- GROUP BY 1, 2, 3
-- ORDER By 4 DESC

-- SELECT nationality, count(*) AS num FROM {{ ref("stg_athletes") }}
-- GROUP BY 1
-- ORDER By 2 DESC