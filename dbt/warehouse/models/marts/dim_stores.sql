{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    store_id,
    name as store_name,
    domain
from {{ ref('stores') }}
