{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    tag_id,
    name
from {{ ref('tags') }}
