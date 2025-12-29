{{
    config(
        materialized="table",
        schema="marts"
    )
}}

-- number of games per genre and how well those games are rated on average

select
    dgenres.genre_id,
    dgenres.genre_name as genre_name,
    count(distinct bg.game_key) as game_count,
    avg(fg.rating) as avg_genre_rating
from {{ ref('bridge_games_genres') }} bg
join {{ ref('dim_genres') }} dgenres
  on bg.genre_id = dgenres.genre_id
join {{ ref('fact_games') }} fg
  on bg.game_key = fg.game_key
group by dgenres.genre_id, dgenres.genre_name