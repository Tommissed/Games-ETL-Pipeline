{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    store_id,
    name,
    domain
from {{ ref('stores') }}
