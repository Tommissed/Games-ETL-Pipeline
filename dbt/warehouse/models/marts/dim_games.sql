{{ 
    config(
        materialized = "table",
        schema = "marts"
    ) 
}}

select
    {{ dbt_utils.generate_surrogate_key(['game_id']) }} as game_key,
    game_id,
    name as game_name,
    slug as game_slug,
    released,
    updated_at
from {{ ref('games') }}

-- to do
-- Dims: dim_games(done), dim_tags(done), dim_genres(done), dim_platforms(done), dim_stores()
-- Facts: fact_games
-- Bridge tables: games_tags, games_genres, games_platforms, games_stores