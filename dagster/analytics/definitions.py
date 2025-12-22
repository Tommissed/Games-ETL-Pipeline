from dagster import Definitions, EnvVar

from analytics.jobs.rawg import run_rawg_etl  # noqa: TID252
from analytics.resources.postgresql import PostgresqlDatabaseResource
from analytics.schedules.rawg import rawg_schedule

defs = Definitions(
    jobs = [run_rawg_etl],
    schedules=[rawg_schedule], #current schedule is set to run every hour
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