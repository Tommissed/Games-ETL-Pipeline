{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    platform_id,
    name as platform_name
from {{ ref('platforms') }}
