WITH
events AS (
    SELECT
        {{ dbt_utils.star(from=ref('events')) }}
    FROM {{ ref('events') }}
),

cast_types AS (
    SELECT
        title,
        strptime(created, '%m/%d/%Y %H:%M:%S') AS created_at_timestamp,
        strptime(changed, '%m/%d/%Y %H:%M:%S') AS changed_at_timestamp,

        -- field_arena_id,
        CAST(event_start_date AS DATE) AS event_start_date,
        EXTRACT(YEAR FROM CAST(event_start_date AS DATE)) AS event_year,
        CAST(event_end_date AS DATE) AS event_end_date,
        -- field_enable_preview,
        field_streaming_available AS is_streaming_available,
        field_twitter_hashtag AS twitter_hashtag,
        field_branding AS branding,
        field_branding_identifier,
        field_event_type,
        field_event_type_identifier,
        field_country AS country,
        city,
        field_program AS program_url,
        field_age AS age_group,
        field_sport AS wrestling_types,
        field_country_noc AS country_code,
        field_results AS results_url,
        CAST(event_session_start_date AS TIMESTAMP) AS session_start_date,
        CAST(event_session_end_date AS TIMESTAMP) AS session_end_date,
        -- alias,
        status
    FROM events
),

add_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
				'title',
                'event_year',
                'country_code',
                'results_url'
			])
		}} as tournament_key,
        *
    FROM cast_types    
),

-- there are tournaments that take place in different locations for:
-- 'wrestling_types',
-- 'age_group',

-- SELECT tournament_key, count(*) FROM add_surrogate_key
-- GROUP BY 1
-- ORDER BY 2 DESC


-- c775bc52a93f461afffb5dbd76692eb2 is a tournament duplicate
remove_duplicates AS (
    SELECT * FROM add_surrogate_key
    QUALIFY ROW_NUMBER() OVER (PARTITION BY 
        title,
        event_year,
        country_code,
        results_url
    ) = 1
)

SELECT * FROM remove_duplicates