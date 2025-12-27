{{
    config(
        materialized="table",
        schema="staging"
    )
}}

select
    genre_id,
    name,
    games_count,
    games
from {{ source('rawg', 'genres') }}
