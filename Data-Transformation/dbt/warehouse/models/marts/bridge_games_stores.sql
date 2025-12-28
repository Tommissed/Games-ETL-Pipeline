{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select

    dg.game_key,                
    dstores.store_id      

from {{ ref('games') }} g
join {{ ref('dim_games') }} dg
  on g.game_id = dg.game_id
, lateral flatten(input => parse_json(g.stores)) stores -- creates a table that has makes one row per element in array 
join {{ ref('dim_stores') }} dstores
  on stores.value:store:id::integer = dstores.store_id -- dig one level deeper than before stores.values:store:id
