{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    genre_id,
    name
from {{ ref('genres') }}
