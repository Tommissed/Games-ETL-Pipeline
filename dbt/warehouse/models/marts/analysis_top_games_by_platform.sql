-- table created for analyses purposes

{{
    config(
        materialized="table",
        schema="marts"
    )
}}

--top rated games per platform

-- dim_platforms
-- bridge_games_platforms -- connect the games tables with the platforms tables
-- dim_games -- has the game names + platform the game is on
-- fact_games -- has the ratings

select
    dp.name as platform_name,
    rank() over (
        partition by dp.platform_id
        order by fg.rating desc
    ) as platform_rank,
    dg.name as game_name,
    fg.rating
from {{ ref('bridge_games_platforms') }} bgp
join {{ ref('dim_platforms') }} dp
  on bgp.platform_id = dp.platform_id
join {{ ref('fact_games') }} fg
  on bgp.game_key = fg.game_key
join {{ ref('dim_games') }} dg
  on fg.game_key = dg.game_key
where fg.rating is not null
ORDER BY fg.rating DESC