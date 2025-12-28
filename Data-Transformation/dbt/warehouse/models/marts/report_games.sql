{{
    config(
        materialized="table",
        schema="marts"
    )
}}

select
    {{ dbt_utils.star(from=ref('fact_games'), relation_alias='fg', except=["game_key"]) }},
    {{ dbt_utils.star(from=ref('dim_games'), relation_alias='dg', except=["game_key"]) }},
    {{ dbt_utils.star(from=ref('dim_platforms'), relation_alias='dp', except=["platform_id"]) }},
    {{ dbt_utils.star(from=ref('dim_genres'), relation_alias='dg2', except=["genre_id"]) }},
    {{ dbt_utils.star(from=ref('dim_stores'), relation_alias='ds', except=["store_id"]) }},
    {{ dbt_utils.star(from=ref('dim_tags'), relation_alias='dt', except=["tag_id"]) }}

from {{ ref('fact_games') }} as fg
left join {{ ref('dim_games') }} as dg on fg.game_key = dg.game_key
left join {{ ref('bridge_games_platforms') }} as bgp on fg.game_key = bgp.game_key
left join {{ ref('dim_platforms') }} as dp on bgp.platform_id = dp.platform_id
left join {{ ref('bridge_games_genres') }} as bgg on fg.game_key = bgg.game_key
left join {{ ref('dim_genres') }} as dg2 on bgg.genre_id = dg2.genre_id
left join {{ ref('bridge_games_stores') }} as bgs on fg.game_key = bgs.game_key
left join {{ ref('dim_stores') }} as ds on bgs.store_id = ds.store_id
left join {{ ref('bridge_games_tags') }} as bgt on fg.game_key = bgt.game_key
left join {{ ref('dim_tags') }} as dt on bgt.tag_id = dt.tag_id