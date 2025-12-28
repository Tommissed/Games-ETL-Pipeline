import requests
import pandas as pd
import datetime

from dagster import (
    Config,
    EnvVar,
    OpExecutionContext,
    asset,
    DailyPartitionsDefinition,
    AutomationCondition,
)

from sqlalchemy import (
    Table,
    Column,
    Integer,
    Text,
    Date,
    Boolean,
    Numeric,
    Float,
    TIMESTAMP,
    MetaData,
)
from sqlalchemy.dialects.postgresql import JSONB

from analytics.resources.postgresql import PostgresqlDatabaseResource
from analytics.ops.common import upsert_to_database


class RAWGApiConfig(Config):
    api_key: str = EnvVar("api_key")
    max_pages: int = 20


# ---GAMES start---
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


daily_partition = DailyPartitionsDefinition(start_date=datetime.datetime(2024, 1, 1))


# extracts individual games from the RAWG API response into a list of dicts 'games'
@asset(
    partitions_def=daily_partition,
    # automation_condition=AutomationCondition.on_cron(cron_schedule="0 0 * * 1-5")
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="* * * * *"
    ),  # runs every minute
)
def raw_games(context: OpExecutionContext, config: RAWGApiConfig) -> list[dict]:
    """
    extracts raw games data from rawg api for given partition date

    args:
        context: OpExecutionContext
        config: RAWGApiConfig

    returns:
        List of dictionaries containing raw games data
    """
    context.log.info("GAMES: Starting RAWG games data extraction")
    page = 1
    non_empty_pages = 0  # implementing a way to track non-empty pages so that we can only extract pages with data
    total_fetched = 0
    games = []

    while non_empty_pages < config.max_pages:
        context.log.info(
            f"Fetching RAWG page {page} for partition {context.partition_key}"
        )
        dt_range = f"{context.partition_key},{context.partition_key}"
        data = fetch_games_page(api_key=config.api_key, dt_range=dt_range, page=page)
        results = data.get("results", [])

        if not results:
            context.log.info(
                f"GAMES: No games found for partition {context.partition_key}, stopping fetch."
            )
            break

        elif results:
            non_empty_pages += 1
            games.extend(results)
            total_fetched += len(results)
            context.log.info(
                f"GAMES: Fetched page {page}, total games: {total_fetched}"
            )

        else:
            context.log.info(f"GAMES: Page {page} is empty, skipping")

        page += 1

        if config.max_pages and page > config.max_pages:
            break

        # break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(
        f"GAMES: Finished fetching RAWG data, total games: {total_fetched}"
    )
    return games


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def transformed_games(context: OpExecutionContext, raw_games: list[dict]) -> list[dict]:
    """
    rransforms the raw games data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        raw_games: List of dictionaries containing raw games data

    returns:
        List of dictionaries containing transformed games data
    """
    context.log.info("GAMES: Starting RAWG data transformation")

    if not raw_games:
        context.log.info(
            "GAMES: No raw games data for this partition. Returning empty list."
        )
        return []

    df = pd.json_normalize(
        raw_games
    )  # using pandas, we normalize the list of dicts into a flat table

    expected_cols = {
        "id",
        "slug",
        "name",
        "released",
        "tba",
        "rating",
        "ratings",
        "rating_top",
        "ratings_count",
        "reviews_text_count",
        "added",
        "added_by_status",
        "metacritic",
        "playtime",
        "suggestions_count",
        "updated",
        "user_game",
        "reviews_count",
        "community_rating",
        "saturated_color",
        "dominant_color",
        "platforms",
        "parent_platform",
        "genres",
        "stores",
        "clip",
        "tags",
        "esrb_rating",
        "short_screenshots",
    }

    missing = expected_cols - set(df.columns)

    if missing:
        context.log.warning(
            f"GAMES: Missing expected RAWG fields for partition {context.partition_key}: {missing}"
        )

    if df.empty:
        context.log.info("GAMES: Normalized dataframe is empty. Returning empty list.")
        return []

    df_renamed = df.rename(
        columns={
            "id": "game_id",
            "slug": "slug",
            "name": "name",
            "released": "released",
            "tba": "tba",
            "background_image": "background_image",
            "rating": "rating",
            "ratings": "ratings",
            "rating_top": "rating_top",
            "ratings_count": "ratings_count",
            "reviews_text_count": "reviews_text_count",
            "added": "added",
            "added_by_status": "added_by_status",
            "metacritic": "metacritic",
            "playtime": "playtime",
            "suggestions_count": "suggestions_count",
            "updated": "updated_at",
            "user_game": "user_game",
            "reviews_count": "reviews_count",
            "community_rating": "community_rating",
            "saturated_color": "saturated_color",
            "dominant_color": "dominant_color",
            "platforms": "platforms",
            "parent_platforms": "parent_platforms",
            "genres": "genres",
            "stores": "stores",
            "clip": "clip",
            "tags": "tags",
            "esrb_rating": "esrb_rating",
            "short_screenshots": "short_screenshots",
        }
    )

    df_selected = df_renamed[
        [
            "game_id",
            "slug",
            "name",
            "released",
            "tba",
            "background_image",
            "rating",
            "ratings",
            "rating_top",
            "ratings_count",
            "reviews_text_count",
            "added",
            "added_by_status",
            "metacritic",
            "playtime",
            "suggestions_count",
            "updated_at",
            "reviews_count",
            "platforms",
            "genres",
            "stores",
            "tags",
            "esrb_rating",
        ]
    ]
    context.log.info("GAMES: Finished RAWG data transformation")
    return df_selected.to_dict(
        orient="records"
    )  # convert the transformed dataframe back to a list of dicts for loading


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def games(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_games=dict,
) -> None:
    """
    loads the transformed games data into the Postgresql database

    args:
        context: OpExecutionContext
        postgres_conn: PostgresqlDatabaseResource
        transformed_games: List of dictionaries containing transformed games data

    returns:
        None
    """
    context.log.info("GAMES: Starting RAWG data loading")

    # stops empty loads
    if not transformed_games:
        context.log.info("GAMES: No transformed games to load. Skipping insert.")
        return

    # construct the metadata
    context.log.info("GAMES: Defining RAWG table metadata")
    metadata = MetaData()
    games = Table(
        "games",
        metadata,
        Column("game_id", Integer, primary_key=True, nullable=False),
        Column("name", Text),
        Column("slug", Text),
        Column("released", Date),
        Column("tba", Boolean),
        Column("background_image", Text),
        Column("rating", Numeric(3, 2)),
        Column("ratings", JSONB),
        Column("rating_top", Numeric(5, 2)),
        Column("ratings_count", Integer),
        Column("reviews_text_count", Integer),
        Column("metacritic", Numeric(5, 2)),
        Column("added", Integer),
        Column("added_by_status", Float),
        Column("playtime", Numeric(5, 2)),
        Column("suggestions_count", Integer),
        Column("updated_at", TIMESTAMP),
        Column("reviews_count", Integer),
        Column("platforms", JSONB),
        Column("genres", JSONB),
        Column("stores", JSONB),
        Column("tags", JSONB),
        Column("esrb_rating", JSONB),
    )
    context.log.info("GAMES: Upsetting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_games,
        table=games,
        metadata=metadata,
    )
    context.log.info("GAMES: Data load complete")


# ---GAMES end---

# ---GENRES start---


@asset(
    partitions_def=daily_partition,
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="* * * * *"
    ),  # runs every minute
)
def raw_genres(context: OpExecutionContext, config: RAWGApiConfig) -> list[dict]:
    """
    extracts raw genres data from rawg api - currently not partitioned by date as genres dont change often

    args:
        context: OpExecutionContext
        config: RAWGApiConfig

    returns:
        List of dictionaries containing raw genres data
    """
    context.log.info("GENRES: Starting RAWG data extraction")
    page = 1
    page_size = 19
    genres = []
    total_fetched = 0

    while True:
        context.log.info("GENRES: Fetching genres")

        url = "https://api.rawg.io/api/genres"
        params = {
            "key": config.api_key,
            "ordering": "added",
            "page": page,
            "page_size": page_size,
        }

        r = requests.get(url, params=params)
        r.raise_for_status()

        data = r.json()
        results = data.get("results", [])

        if not results:
            context.log.info("GENRES: No genres found, stopping fetch.")
            break

        elif results:
            genres.extend(results)
            total_fetched += len(results)
            context.log.info(
                f"GENRES: Fetched page {page}, total genres: {total_fetched}"
            )

        else:
            context.log.info(f"GENRES: Page {page} is empty, skipping")

        # break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(
        f"GENRES: Finished fetching RAWG data, total genres: {total_fetched}"
    )
    return genres


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def transformed_genres(
    context: OpExecutionContext, raw_genres: list[dict]
) -> list[dict]:
    """
    rransforms the raw genres data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        raw_genres: List of dictionaries containing raw genre data

    returns:
        List of dictionaries containing transformed genre data
    """
    context.log.info("GENRES: Starting RAWG data transformation")

    if not raw_genres:
        context.log.info(
            "GENRES: No raw genres data for this partition. Returning empty list."
        )
        return []

    df = pd.json_normalize(
        raw_genres
    )  # using pandas, we normalize the list of dicts into a flat table

    expected_cols = {"id", "name", "slug", "games_count", "image_background", "games"}

    missing = expected_cols - set(df.columns)

    if missing:
        context.log.warning(f"GENRES: Missing expected RAWG fields: {missing}")

    if df.empty:
        context.log.info("GENRES: Normalized dataframe is empty. Returning empty list.")
        return []

    df_renamed = df.rename(
        columns={
            "id": "genre_id",
            "name": "name",
            "slug": "slug",
            "games_count": "games_count",
            "image_background": "image_background",
            "games": "games",
        }
    )
    df_selected = df_renamed[
        ["genre_id", "name", "slug", "games_count", "image_background", "games"]
    ]
    context.log.info("GENRES: Finished RAWG data transformation")
    return df_selected.to_dict(
        orient="records"
    )  # convert the transformed dataframe back to a list of dicts for loading


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def genres(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_genres=dict,
) -> None:
    """
    loads the transformed genres data into the Postgresql database

    args:
        context: OpExecutionContext
        postgres_conn: PostgresqlDatabaseResource
        transformed_games: List of dictionaries containing transformed genres data

    returns:
        None
    """
    context.log.info("GENRES: Starting RAWG data loading")

    # stops empty loads
    if not transformed_genres:
        context.log.info("GENRES: No transformed genres to load. Skipping insert.")
        return

    # construct the metadata
    context.log.info("GENRES: Defining RAWG table metadata")
    metadata = MetaData()
    genres = Table(
        "genres",
        metadata,
        Column("genre_id", Integer, primary_key=True, nullable=False),
        Column("name", Text),
        Column("slug", Text),
        Column("games_count", Integer),
        Column("image_background", Text),
        Column("games", JSONB),
    )
    context.log.info("GENRES: Upsetting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_genres,
        table=genres,
        metadata=metadata,
    )
    context.log.info("GENRES: Data load complete")


# ---GENRES end---

# ---PLATFORMS start---


@asset(
    partitions_def=daily_partition,
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="* * * * *"
    ),  # runs every minute
)
def raw_platforms(context: OpExecutionContext, config: RAWGApiConfig) -> list[dict]:
    """
    extracts raw platforms data from rawg api - currently not partitioned by date as platforms dont change often

    args:
        context: OpExecutionContext
        config: RAWGApiConfig

    returns:
        List of dictionaries containing raw platforms data
    """
    context.log.info("PLATFORMS: Starting RAWG data extraction")
    page = 1
    page_size = 40
    platforms = []
    total_fetched = 0

    while True:
        context.log.info("PLATFORMS: Fetching platforms")

        url = "https://api.rawg.io/api/platforms"
        params = {
            "key": config.api_key,
            "ordering": "added",
            "page": page,
            "page_size": page_size,
        }

        r = requests.get(url, params=params)
        r.raise_for_status()

        data = r.json()
        results = data.get("results", [])

        if not results:
            context.log.info("PLATFORMS: No platforms found, stopping fetch.")
            break

        elif results:
            platforms.extend(results)
            total_fetched += len(results)
            context.log.info(
                f"PLATFORMS: Fetched page {page}, total platforms: {total_fetched}"
            )

        else:
            context.log.info(f"PLATFORMS: Page {page} is empty, skipping")

        page += 1

        # break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(
        f"PLATFORMS: Finished fetching RAWG data, total platforms: {total_fetched}"
    )
    return platforms


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def transformed_platforms(
    context: OpExecutionContext, raw_platforms: list[dict]
) -> list[dict]:
    """
    rransforms the raw platforms data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        raw_platforms: List of dictionaries containing raw platforms data

    returns:
        List of dictionaries containing transformed platforms data
    """
    context.log.info("PLATFORMS: Starting RAWG data transformation")

    if not raw_platforms:
        context.log.info(
            "PLATFORMS: No raw platforms data for this partition. Returning empty list."
        )
        return []

    df = pd.json_normalize(
        raw_platforms
    )  # using pandas, we normalize the list of dicts into a flat table

    expected_cols = {
        "id",
        "name",
        "slug",
        "games_count",
        "image_background",
        "image",
        "year_start",
        "year_end",
        "games",
    }

    missing = expected_cols - set(df.columns)

    if missing:
        context.log.warning(f"PLATFORMS: Missing expected RAWG fields: {missing}")

    if df.empty:
        context.log.info(
            "PLATFORMS: Normalized dataframe is empty. Returning empty list."
        )
        return []

    df_renamed = df.rename(
        columns={
            "id": "platform_id",
            "name": "name",
            "slug": "slug",
            "games_count": "games_count",
            "image_background": "image_background",
            "image": "image",
            "year_start": "year_start",
            "year_end": "year_end",
            "games": "games",
        }
    )
    df_selected = df_renamed[
        ["platform_id", "name", "slug", "games_count", "image_background", "games"]
    ]
    context.log.info("PLATFORMS: Finished RAWG data transformation")
    return df_selected.to_dict(
        orient="records"
    )  # convert the transformed dataframe back to a list of dicts for loading


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def platforms(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_platforms=dict,
) -> None:
    """
    loads the transformed platforms data into the Postgresql database

    args:
        context: OpExecutionContext
        postgres_conn: PostgresqlDatabaseResource
        transformed_games: List of dictionaries containing transformed platforms data

    returns:
        None
    """
    context.log.info("PLATFORMS: Starting RAWG data loading")

    # stops empty loads
    if not transformed_platforms:
        context.log.info(
            "PLATFORMS: No transformed platforms to load. Skipping insert."
        )
        return

    # construct the metadata
    context.log.info("PLATFORMS: Defining RAWG table metadata")
    metadata = MetaData()
    platforms = Table(
        "platforms",
        metadata,
        Column("platform_id", Integer, primary_key=True, nullable=False),
        Column("name", Text),
        Column("slug", Text),
        Column("games_count", Integer),
        Column("image_background", Text),
        Column("games", JSONB),
    )
    context.log.info("PLATFORMS: Upsetting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_platforms,
        table=platforms,
        metadata=metadata,
    )
    context.log.info("PLATFORMS: Data load complete")


# ---PLATFORMS end---

# ---STORES start---


@asset(
    partitions_def=daily_partition,
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="* * * * *"
    ),  # runs every minute
)
def raw_stores(context: OpExecutionContext, config: RAWGApiConfig) -> list[dict]:
    """
    extracts raw stores data from rawg api - currently not partitioned by date as stores dont change often

    args:
        context: OpExecutionContext
        config: RAWGApiConfig

    returns:
        List of dictionaries containing raw stores data
    """
    context.log.info("STORES: Starting RAWG data extraction")
    page = 1
    page_size = 40
    stores = []
    total_fetched = 0

    while True:
        context.log.info("STORES: Fetching stores")

        url = "https://api.rawg.io/api/stores"
        params = {
            "key": config.api_key,
            "ordering": "added",
            "page": page,
            "page_size": page_size,
        }

        r = requests.get(url, params=params)
        r.raise_for_status()

        data = r.json()
        results = data.get("results", [])

        if not results:
            context.log.info("STORES: No stores found, stopping fetch.")
            break

        elif results:
            stores.extend(results)
            total_fetched += len(results)
            context.log.info(
                f"STORES: Fetched page {page}, total stores: {total_fetched}"
            )

        else:
            context.log.info(f"STORES: Page {page} is empty, skipping")

        page += 1

        # break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(
        f"STORES: Finished fetching RAWG data, total stores: {total_fetched}"
    )
    return stores


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def transformed_stores(
    context: OpExecutionContext, raw_stores: list[dict]
) -> list[dict]:
    """
    rransforms the raw stores data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        raw_stores: List of dictionaries containing raw stores data

    returns:
        List of dictionaries containing transformed stores data
    """
    context.log.info("STORES: Starting RAWG data transformation")

    if not raw_stores:
        context.log.info(
            "STORES: No raw stores data for this partition. Returning empty list."
        )
        return []

    df = pd.json_normalize(
        raw_stores
    )  # using pandas, we normalize the list of dicts into a flat table

    expected_cols = {
        "id",
        "name",
        "domain",
        "slug",
        "games_count",
        "image_background",
        "games",
    }

    missing = expected_cols - set(df.columns)

    if missing:
        context.log.warning(f"STORES: Missing expected RAWG fields: {missing}")

    if df.empty:
        context.log.info("STORES: Normalized dataframe is empty. Returning empty list.")
        return []

    df_renamed = df.rename(
        columns={
            "id": "store_id",
            "name": "name",
            "domain": "domain",
            "slug": "slug",
            "games_count": "games_count",
            "image_background": "image_background",
            "games": "games",
        }
    )
    df_selected = df_renamed[
        [
            "store_id",
            "name",
            "domain",
            "slug",
            "games_count",
            "image_background",
            "games",
        ]
    ]
    context.log.info("STORES: Finished RAWG data transformation")
    return df_selected.to_dict(
        orient="records"
    )  # convert the transformed dataframe back to a list of dicts for loading


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def stores(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_stores=dict,
) -> None:
    """
    loads the transformed stores data into the Postgresql database

    args:
        context: OpExecutionContext
        postgres_conn: PostgresqlDatabaseResource
        transformed_games: List of dictionaries containing transformed stores data

    returns:
        None
    """
    context.log.info("STORES: Starting RAWG data loading")

    # stops empty loads
    if not transformed_stores:
        context.log.info("STORES: No transformed stores to load. Skipping insert.")
        return

    # construct the metadata
    context.log.info("STORES: Defining RAWG table metadata")
    metadata = MetaData()
    stores = Table(
        "stores",
        metadata,
        Column("store_id", Integer, primary_key=True, nullable=False),
        Column("name", Text),
        Column("domain", Text),
        Column("slug", Text),
        Column("games_count", Integer),
        Column("image_background", Text),
        Column("games", JSONB),
    )
    context.log.info("STORES: Upsetting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_stores,
        table=stores,
        metadata=metadata,
    )
    context.log.info("STORES: Data load complete")


# ---STORES end---


# ---TAGS start---
# @helper function
def fetch_tags_page(api_key, page: int = 1, page_size: int = 40) -> dict:
    """
    Fetches a single page of tags from the RAWG API.

    Args:
        api_key: RAWG API key
        page: The page number to fetch
        page_size: Number of results per page

    Returns:
        Dictionary of 40 games from the response json
    """
    url = "https://api.rawg.io/api/tags"
    params = {
        "key": api_key,
        "ordering": "added",  # sorting extracted data by release date (other option includes -updated for most recently updated)
        "page": page,
        "page_size": page_size,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()


# 244 pages is max (total number of tags= 9722)
@asset(
    partitions_def=daily_partition,
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="* * * * *"
    ),  # runs every minute
)
def raw_tags(context: OpExecutionContext, config: RAWGApiConfig) -> list[dict]:
    """
    extracts raw tags data from rawg api - currently not partitioned by date as tags dont change often

    args:
        context: OpExecutionContext
        config: RAWGApiConfig

    returns:
        List of dictionaries containing raw tags data
    """
    context.log.info("TAGS: Starting RAWG data extraction")
    page = 1
    tags = []
    total_fetched = 0
    non_empty_pages = 0

    while non_empty_pages < config.max_pages:
        context.log.info("TAGS: Fetching tags")

        data = fetch_tags_page(api_key=config.api_key, page=page)
        results = data.get("results", [])

        if not results:
            context.log.info("TAGS: No tags found, stopping fetch.")
            break

        elif results:
            non_empty_pages += 1
            tags.extend(results)
            total_fetched += len(results)
            context.log.info(f"TAGS: Fetched page {page}, total tags: {total_fetched}")

        else:
            context.log.info(f"TAGS: Page {page} is empty, skipping")

        page += 1

        if config.max_pages and page > config.max_pages:
            break

        # break if there are no more valid pages so that the code doesnt loop infinitely
        if not data.get("next"):
            break

    context.log.info(f"TAGS: Finished fetching RAWG data, total tags: {total_fetched}")
    return tags


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def transformed_tags(context: OpExecutionContext, raw_tags: list[dict]) -> list[dict]:
    """
    rransforms the raw tags data into a more suitable format for loading into the database

    args:
        context: OpExecutionContext
        raw_tags: List of dictionaries containing raw tags data

    returns:
        List of dictionaries containing transformed tags data
    """
    context.log.info("TAGS: Starting RAWG data transformation")

    if not raw_tags:
        context.log.info(
            "TAGS: No raw tags data for this partition. Returning empty list."
        )
        return []

    df = pd.json_normalize(
        raw_tags
    )  # using pandas, we normalize the list of dicts into a flat table

    expected_cols = {
        "id",
        "name",
        "slug",
        "games_count",
        "image_background",
        "language",
        "games",
    }

    missing = expected_cols - set(df.columns)

    if missing:
        context.log.warning(f"TAGS: Missing expected RAWG fields: {missing}")

    if df.empty:
        context.log.info("TAGS: Normalized dataframe is empty. Returning empty list.")
        return []

    df_renamed = df.rename(
        columns={
            "id": "tag_id",
            "name": "name",
            "slug": "slug",
            "games_count": "games_count",
            "image_background": "image_background",
            "language": "language",
            "games": "games",
        }
    )
    df_selected = df_renamed[
        [
            "tag_id",
            "name",
            "slug",
            "games_count",
            "image_background",
            "language",
            "games",
        ]
    ]
    context.log.info("TAGS: Finished RAWG data transformation")
    return df_selected.to_dict(
        orient="records"
    )  # convert the transformed dataframe back to a list of dicts for loading


@asset(partitions_def=daily_partition, automation_condition=AutomationCondition.eager())
def tags(
    context: OpExecutionContext,
    postgres_conn: PostgresqlDatabaseResource,
    transformed_tags=dict,
) -> None:
    """
    loads the transformed tags data into the Postgresql database

    args:
        context: OpExecutionContext
        postgres_conn: PostgresqlDatabaseResource
        transformed_games: List of dictionaries containing transformed tags data

    returns:
        None
    """
    context.log.info("TAGS: Starting RAWG data loading")

    # stops empty loads
    if not transformed_tags:
        context.log.info("TAGS: No transformed tags to load. Skipping insert.")
        return

    # construct the metadata
    context.log.info("TAGS: Defining RAWG table metadata")
    metadata = MetaData()
    tags = Table(
        "tags",
        metadata,
        Column("tag_id", Integer, primary_key=True, nullable=False),
        Column("name", Text),
        Column("slug", Text),
        Column("games_count", Integer),
        Column("image_background", Text),
        Column("language", Text),
        Column("games", JSONB),
    )
    context.log.info("TAGS: Upsetting RAWG data into database")
    upsert_to_database(
        postgres_conn=postgres_conn,
        data=transformed_tags,
        table=tags,
        metadata=metadata,
    )
    context.log.info("TAGS: Data load complete")
