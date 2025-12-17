#using rawg video games database api
import requests
from dotenv import load_dotenv
from connectors.postgres import PostgreSqlClient 
from sqlalchemy import (
    Table, Column, Integer, Text, Date, Boolean, Numeric,
    TIMESTAMP, MetaData
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY
from datetime import datetime
import os

load_dotenv()

# def get_games() -> dict:
#     api_key = os.getenv("api_key")
#     base_url = "https://api.rawg.io/api"

#     params = {
#         "key": api_key,
#         "ordering": "-updated"
#     }
#     response_json=requests.get(url=f"{base_url}/games", params=params) 

#     return response_json.json()
#     #print(response_json.json())

def fetch_games_page(api_key, page=1, page_size=40):
    url = "https://api.rawg.io/api/games"
    params = {
        "key": api_key,
        "ordering": "-updated",
        "page": page,
        "page_size": page_size
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def get_all_games(api_key, max_pages=None):
    all_games = []
    page = 1

    while True:
        data = fetch_games_page(api_key, page=page)
        results = data["results"]

        if not results:
            break

        all_games.extend(results)
        print(f"Fetched page {page}, total games: {len(all_games)}")

        if max_pages and page >= max_pages:
            break

        if not data.get("next"):
            break

        page += 1

    return all_games

def load_games(PostgreSqlClient=PostgreSqlClient, list=list):
    metadata=MetaData()

    #construct the metadata
    raw_games = Table(
        "raw_games",
        metadata,

        Column("game_id", Integer, primary_key=True),
        Column("slug", Text),
        Column("name", Text),
        Column("released", Date),
        Column("tba", Boolean),

        Column("rating", Numeric(3, 2)),
        Column("rating_top", Integer),
        Column("ratings_count", Integer),
        Column("reviews_count", Integer),
        Column("metacritic", Integer),

        Column("playtime", Integer),

        Column("updated_at", TIMESTAMP),

        # Nested fields
        Column("platforms", JSONB),
        Column("genres", JSONB),
        Column("developers", JSONB),
        Column("publishers", JSONB),
        Column("stores", JSONB),
        Column("tags", JSONB),
        Column("ratings", JSONB),
        Column("short_screenshots", JSONB),

        # Raw payload (always keep this)
        Column("raw_payload", JSONB, nullable=False),

        Column("extracted_at", TIMESTAMP),
    )

    #creates the table if does not exist
    metadata.create_all(PostgreSqlClient.engine)

    rows = [transform_game(game) for game in list]

    with PostgreSqlClient.engine.begin() as conn: # opens a trasaction
        #have to create the insert statement first to then create upsert statement

        for i in range(0,len(rows),1000):
            sub_list = rows[i:i+1000]
            insert_statement=postgresql.insert(raw_games).values(sub_list)
            
            upsert_statement =insert_statement.on_conflict_do_update(
                index_elements=['game_id'],
                #for each column not part of the conflict key, update it to the new value
                set_={c.key: c for c in insert_statement.excluded if c.key not in ['game_id']})

            try:
                conn.execute(upsert_statement)
                print(f"Inserted {i}-{min(i+1000, len(rows))}")
            except Exception as e:
                print(f"❌ Error at chunk {i}-{min(i+1000, len(rows))}: {e}")

    print('uploaded to database')

    return

def transform_game(game: dict) -> dict:
    return {
        "game_id": game.get("id"),
        "slug": game.get("slug"),
        "name": game.get("name"),
        "released": game.get("released"),
        "tba": game.get("tba"),

        "rating": game.get("rating"),
        "rating_top": game.get("rating_top"),
        "ratings_count": game.get("ratings_count"),
        "reviews_count": game.get("reviews_count"),
        "metacritic": game.get("metacritic"),

        "playtime": game.get("playtime"),
        "updated_at": game.get("updated"),

        "platforms": game.get("platforms"),
        "genres": game.get("genres"),
        "developers": game.get("developers"),
        "publishers": game.get("publishers"),
        "stores": game.get("stores"),
        "tags": game.get("tags"),
        "ratings": game.get("ratings"),
        "short_screenshots": game.get("short_screenshots"),

        "raw_payload": game,
        "extracted_at": datetime.utcnow()
    }

#database details
DB_SERVER_NAME= os.environ.get("DB_SERVER_NAME")
DB_DATABASE_NAME = os.environ.get("DB_DATABASE_NAME")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT")

api_key = os.environ.get("api_key")

postgres_client=PostgreSqlClient(db_server_name=DB_SERVER_NAME,
                                db_database_name=DB_DATABASE_NAME,
                                db_username=DB_USERNAME,
                                db_password=DB_PASSWORD,
                                db_port=DB_PORT)


all_games = get_all_games(api_key, max_pages=5)
load_games(PostgreSqlClient=postgres_client, list=all_games)