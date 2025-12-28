{{
    config(
        materialized="table",
        schema="marts"
    )
}}

-- bridge tables do not serve any other purpose other than linking two tables together.
-- therefore, i should remove all the columns that provide context or facts.
-- using this table, and joining with fact_games, dim_games and dim_genres,
-- I'll be able to see what games are connected to what genre per row.

select

    dg.game_key,  -- allows me to join to dim_games later
    dgenres.genre_id     -- allows me to join to dim_genres later

from {{ ref('games') }} g -- contains the column with the nested json of genres
join {{ ref('dim_games') }} dg -- contains the surrogate_key used to identify games

  on g.game_id = dg.game_id -- join dim_games and games - now i have a table containing all my games, with their game_key and genres JSON
, lateral flatten(input => parse_json(g.genres)) genres -- flatten the json of genres
join {{ ref('dim_genres') }} dgenres -- match the genre_id from the flattened json to the id of the genre within dim_genres
  on genres.value:id::integer = dgenres.genre_id
