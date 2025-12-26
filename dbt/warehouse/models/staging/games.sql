{{
    config(
        materialized="incremental",
        unique_key=["game_id"],
        schema="staging",
        incremental_strategy="delete+insert"
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
from {{ source('rawg', 'games') }} -- see sources.yml: rawg is the name of the source not the name of the server, games is the table

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }} )
{% endif %}
