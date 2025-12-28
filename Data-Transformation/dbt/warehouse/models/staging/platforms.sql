{{
    config(
        materialized="table",
        schema="staging"
    )
}}

select
    platform_id,
    name,
    games_count,
    games
from {{ source('rawg', 'platforms') }}
