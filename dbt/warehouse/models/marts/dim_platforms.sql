{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    platform_id,
    name
from {{ ref('platforms') }}
