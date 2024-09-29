WITH
events AS (
    SELECT *
    FROM {{ ref('events') }}
),

cast_types AS (
    SELECT
        title,
        created,
        changed,
        -- field_arena_id,
        CAST(event_start_date AS DATE) AS event_start_date,
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
)



SELECT * FROM cast_types