{{
    config(
        materialized="incremental",
        unique_key=["game_id"],
        schema="staging",
        incremental_strategy="delete+insert"
        )
}}

select
    GAME_ID,
    NAME,
    SLUG,
    TBA,
    RATING,
    STORES,
    GENRES,
    TAGS,
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
