{{
    config(
        materialized="table"
        )
}}

select
    -- _AIRBYTE_RAW_ID,
    -- _AIRBYTE_EXTRACTED_AT,
    -- _AIRBYTE_META,
    -- _AIRBYTE_GENERATION_ID,
    TBA,
    NAME,
    SLUG,
    RATING,
    GAME_ID,
    RATINGS,
    PLAYTIME,
    RELEASED,
    PLATFORMS,
    METACRITIC,
    RATING_TOP,
    UPDATED_AT,
    RATINGS_COUNT,
    REVIEWS_TEXT_COUNT
from {{ ref('games') }}
limit 10