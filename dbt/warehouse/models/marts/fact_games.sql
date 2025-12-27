{{ 
    config(
        materialized = "table",
        schema = "marts"
    ) 
}}

-- think to use CTEs to filter for only the columns i need before i join tables.

select
    dg.game_key, -- FK to dim_games
    g.rating,
    g.ratings,
    g.ratings_count,
    g.reviews_text_count,
    g.playtime,
    g.metacritic
from {{ ref('games') }} g
join {{ ref('dim_games') }} dg
  on g.game_id = dg.game_id
