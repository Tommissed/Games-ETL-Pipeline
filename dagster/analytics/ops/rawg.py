import requests
import pandas as pd

from dagster import op, Config, EnvVar, OpExecutionContext

from sqlalchemy import (
    Table,
    Column,
    Integer,
    Text,
    Date,
    Boolean,
    Numeric,
    TIMESTAMP,
    MetaData,
)
from sqlalchemy.dialects.postgresql import JSONB

from analytics.resources.postgresql import PostgresqlDatabaseResource
from analytics.ops.common import upsert_to_database


class RAWGApiConfig(Config):
    api_key: str = EnvVar("api_key")
    date: str


# gets a page of games from the RAWG API
# @helper function
def fetch_games_page(api_key, dt_range, page: int = 1, page_size: int = 40) -> dict:
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
        "ordering": "released",  # sorting extracted data by release date (other option includes -updated for most recently updated)
        "page": page,
        "page_size": page_size,
        "dates": dt_range,  # rawg api expects date range in the format YYYY-MM-DD,YYYY-MM-DD
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()


# extracts individual games from the RAWG API response into a list of dicts 'games'
@op
def extract_rawg(
    context: OpExecutionContext, config: RAWGApiConfig, max_pages=5
) -> list[dict]:
    context.log.info("Starting RAWG data extraction")
    page = 1
    non_empty_pages = 0  # implementing a way to track non-empty pages so that we can only extract pages with data
    total_fetched = 0
    games = []

    while non_empty_pages < max_pages:
        context.log.info(f"Fetching RAWG page {page}")
        dt_range = f"{config.date},{config.date}"
        data = fetch_games_page(api_key=config.api_key, dt_range=dt_range, page=page)
        results = data.get("results", [])

        if results:
            non_empty_pages += 1
            games.extend(results)
            total_fetched += len(results)
            context.log.info(f"Fetched page {page}, total games: {total_fetched}")

        else:
            context.log.info(f"Page {page} is empty, skipping")

        page += 1

        if max_pages and page > max_pages:
            break

        # break if there are no more valid pages so that the code doesnt loop infinitely.
        if not data.get("next"):
            break

    context.log.info(f"Finished fetching RAWG data, total games: {total_fetched}")
    return games


@op
def transform_rawg(context: OpExecutionContext, games: list[dict]) -> list[dict]:
    context.log.info("Starting RAWG data transformation")
    df = pd.json_normalize(games)
    df_renamed = df.rename(
        columns={
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
        }
    )
    df_selected = df_renamed[
        [
            "game_id",
            "slug",
            "name",
            "released",
            "tba",
            "rating",
            "ratings",
            "rating_top",
            "ratings_count",
            "reviews_text_count",
            "metacritic",
            "playtime",
            "updated_at",
            "platforms",
        ]
    ]
    context.log.info("Finished RAWG data transformation")
    return df_selected.to_dict(orient="records")


@op
def load_rawg(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_game=dict,
) -> None:
    context.log.info("Starting RAWG data loading")

    # construct the metadata
    context.log.info("Defining RAWG table metadata")
    metadata = MetaData()
    games = Table(
        "games",
        metadata,
        Column("game_id", Integer, primary_key=True),
        Column("slug", Text),
        Column("name", Text),
        Column("released", Date),
        Column("tba", Boolean),
        Column("rating", Numeric(3, 2)),
        Column("ratings", JSONB),
        Column("rating_top", Numeric(5, 2)),
        Column("ratings_count", Integer),
        Column("reviews_text_count", Integer),
        Column("metacritic", Numeric(5, 2)),
        Column("playtime", Numeric(5, 2)),
        Column("updated_at", TIMESTAMP),
        Column("platforms", JSONB),
    )
    context.log.info("Upserting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_game,
        table=games,
        metadata=metadata,
    )
    context.log.info("Data load complete")
