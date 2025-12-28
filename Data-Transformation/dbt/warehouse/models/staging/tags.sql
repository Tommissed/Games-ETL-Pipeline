{{
    config(
        materialized="incremental",
        unique_key=["tag_id"],
        schema="staging",
        incremental_strategy="delete+insert"
        )
}}

select
    tag_id,
    name,
    games_count
from {{ source('rawg', 'tags') }}

--behavior : dbt compares incomign rows with what i have in my table already
-- any rows with matching tag_ids will then be deleted from my target table
-- the new data is then inserted into the database, updating the rows that were deleted and any that didnt exist.