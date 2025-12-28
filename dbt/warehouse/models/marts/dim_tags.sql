{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    tag_id,
    name as tag_name
from {{ ref('tags') }}
