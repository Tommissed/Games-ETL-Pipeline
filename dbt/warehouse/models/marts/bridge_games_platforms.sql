{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select

    dg.game_key,                
    dplatforms.platform_id      

from {{ ref('games') }} g
join {{ ref('dim_games') }} dg
  on g.game_id = dg.game_id
join lateral flatten(input => parse_json(g.platforms)) platforms -- creates a table that has makes one row per element in array 
  on true
join {{ ref('dim_platforms') }} dplatforms
  on platforms.value:platform:id::integer = dplatforms.platform_id -- dig one level deeper than before platformms.values:platform:id
