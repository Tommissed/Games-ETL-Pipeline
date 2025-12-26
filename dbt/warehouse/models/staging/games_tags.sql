{{
    config(
        materialized="table"
    )
}}

select
    g.game_id,
    t.value:id::integer   as tag_id,
    t.value:name::string as tag_name
from {{ source('rawg', 'games') }} g,
     lateral flatten(input => parse_json(g.tags)) t
