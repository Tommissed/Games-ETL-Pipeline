{{
    config(
        materialized="table",
        schema="staging"
    )
}}

select
    tags_id,
    name,
    games_count
from {{ source('rawg', 'tags') }}
