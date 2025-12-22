import requests
import pandas as pd
import datetime

from dagster import op, Config, EnvVar, OpExecutionContext, asset, TimeWindowPartitionedDefinition

from sqlalchemy import (
    Table, Column, Integer, Text, Date, Boolean, Numeric,
    TIMESTAMP, MetaData, URL, create_engine
)
from sqlalchemy.dialects.postgresql import JSONB

from analytics.resources.postgresql import PostgresqlDatabaseResource
from analytics.ops.common import upsert_to_database

class RAWGApiConfig(Config):
    api_key: str = EnvVar("api_key")

weekday_partition = TimeWindowPartitionedDefinition(start=datetime.date(2024, 1, 1), fmt="%Y-%m-%d", cron_schedule="0 0 * * 1-5")

#gets a page of games from the RAWG API
#@helper function
def fetch_games_page(api_key, dt_range, page:int=1, page_size:int=40) -> dict:
    """
    Fetches a single page of games from the RAWG API.

    Args:
        api_key: RAWG API key
        dt_range: Date timestamp to filter games
        page: The page number to fetch
        page_size: Number of results per page

    Returns:
        Dictionary of 40 games from the response json
    """
    url = "https://api.rawg.io/api/games"
    params = {
        "key": api_key,
        "ordering": "released", #sorting extracted data by release date (other option includes -updated for most recently updated)
        "page": page,
        "page_size": page_size,
        "dates": dt_range #rawg api expects date range in the format YYYY-MM-DD,YYYY-MM-DD
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

#extracts individual games from the RAWG API response into a list of dicts 'games'
@asset(
        partitions_def=weekday_partition,
)
def raw_games(context: OpExecutionContext, config: RAWGApiConfig, max_pages = 5) -> list[dict]:
    """
    extracts raw games data from rawg api for given partition date

    args:
        context: OpExecutionContext
        config: RAWGApiConfig
        max_pages: maximum number of pages to fetch from the API

    returns:
        List of dictionaries containing raw games data
    """
    context.log.info("Starting RAWG data extraction")
    page = 1
    non_empty_pages = 0 #implementing a way to track non-empty pages so that we can only extract pages with data
    total_fetched = 0
    games = []

    while non_empty_pages < max_pages:
        context.log.info(f"Fetching RAWG page {page} for partition {context.partition_key}")
        dt_range = f"{context.partition_key},{context.partition_key}"
        data = fetch_games_page(api_key=config.api_key, dt_range=dt_range, page=page)
        results = data.get("results", [])

        if results:
            non_empty_pages += 1
            games.extend(results)
            total_fetched += len(results)
            context.log.info(f"Fetched page {page}, total games: {total_fetched}")

        else:
            context.log.info(f"Page {page} is empty, skipping")

        page +=1

        if max_pages and page > max_pages:
            break

        #break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(f"Finished fetching RAWG data, total games: {total_fetched}")
    return games

@asset(
        partitions_def=weekday_partition,
)
def transformed_games(context: OpExecutionContext, games: list[dict]) -> list[dict]:
    """
    rransforms the raw games data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        games: List of dictionaries containing raw games data

    returns:
        List of dictionaries containing transformed games data
    """
    context.log.info("Starting RAWG data transformation")
    df = pd.json_normalize(games) #using pandas, we normalize the list of dicts into a flat table
    df_renamed = df.rename(columns={
        "id": "game_id",
        "slug": "slug",
        "name": "name",
        "released": "released",
        "tba": "tba",
        "rating": "rating",
        "ratings": "ratings",
        "rating_top": "rating_top",
        "ratings_count": "ratings_count",
        "reviews_text_count": "reviews_text_count",
        "metacritic": "metacritic",
        "playtime": "playtime",
        "updated": "updated_at",
        "platforms": "platforms",
    })
    df_selected = df_renamed[["game_id", "slug", "name", "released", "tba",
                              "rating", "ratings", "rating_top", "ratings_count", "reviews_text_count", "metacritic",
                              "playtime", "updated_at", "platforms"]]
    context.log.info("Finished RAWG data transformation")
    return df_selected.to_dict(orient="records") #convert the transformed dataframe back to a list of dicts for loading

@asset(
        partitions_def=weekday_partition,
)
def games(context: OpExecutionContext, postgres_conn: PostgresqlDatabaseResource, transformed_games=dict) -> None:
    context.log.info("Starting RAWG data loading")

    #construct the metadata
    context.log.info("Defining RAWG table metadata")
    metadata=MetaData()
    raw_games = Table(
        "raw_games",
        metadata,

        Column("game_id", Integer, primary_key=True),
        Column("slug", Text),
        Column("name", Text),
        Column("released", Date),
        Column("tba", Boolean),

        Column("rating", Numeric(3, 2)),
        Column("ratings", JSONB),
        Column("rating_top", Numeric(5,2)),
        Column("ratings_count", Integer),
        Column("reviews_text_count", Integer),
        Column("metacritic", Numeric(5,2)),

        Column("playtime", Numeric(5,2)),

        Column("updated_at", TIMESTAMP),


        Column("platforms", JSONB),
    )
    context.log.info("Upserting RAWG data into database")
    upsert_to_database(postgres_conn = postgres_conn, data=transformed_games, table=raw_games, metadata=metadata)
    context.log.info("Data load complete")