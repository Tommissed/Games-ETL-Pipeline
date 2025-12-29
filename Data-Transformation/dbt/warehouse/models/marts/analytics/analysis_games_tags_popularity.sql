{{
    config(
        materialized="table",
        schema="marts"
    )
}}

-- number of times a tag is featured

select
    dt.tag_id,
    dt.tag_name as tag_name,
    count(*) as tagged_game_count
from {{ ref('bridge_games_tags') }} bgt
join {{ ref('dim_tags') }} dt
  on bgt.tag_id = dt.tag_id
group by dt.tag_id, dt.tag_name
order by tagged_game_count desc
