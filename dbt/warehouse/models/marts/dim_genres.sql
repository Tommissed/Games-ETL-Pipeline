{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    genre_id,
    name as genre_name
from {{ ref('genres') }}
