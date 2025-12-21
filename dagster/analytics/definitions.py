from dagster import Definitions, EnvVar

from analytics.jobs.rawg import run_rawg_etl  # noqa: TID252
from analytics.resources.postgresql import PostgresqlDatabaseResource

defs = Definitions(
    jobs = [run_rawg_etl],
    resources = {
        "postgres_conn": PostgresqlDatabaseResource(
            DB_SERVER_NAME=EnvVar("DB_SERVER_NAME"),
            DB_DATABASE_NAME=EnvVar("DB_DATABASE_NAME"),
            DB_USERNAME=EnvVar("DB_USERNAME"),
            DB_PASSWORD=EnvVar("DB_PASSWORD"),
            DB_PORT=EnvVar("DB_PORT")
        )
    }
)