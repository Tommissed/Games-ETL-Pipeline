{{
    config(
        materialized="table",
        schema="staging"
    )
}}

select
    store_id,
    name,
    domain,
    games,
    games_count
from {{ source('rawg', 'stores') }}
