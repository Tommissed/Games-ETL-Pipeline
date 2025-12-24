from dagster import Definitions, EnvVar, load_assets_from_modules

from analytics.jobs.rawg import run_rawg_etl  # noqa: TID252
from analytics.resources.postgresql import PostgresqlDatabaseResource
from analytics.schedules.rawg import rawg_schedule
from analytics.assets import rawg
from analytics.assets.airbyte import all_airbyte_assets, airbyte_workspace
from analytics.assets.dbt import dbt_warehouse, dbt_warehouse_resource

rawg_assets = load_assets_from_modules([rawg], group_name="rawg_postgres", key_prefix="postgres")

defs = Definitions(
    assets=[*rawg_assets, *all_airbyte_assets, dbt_warehouse],
    jobs = [run_rawg_etl],
    schedules=[rawg_schedule], #current schedule is set to run every hour
    resources = {
        "postgres_conn": PostgresqlDatabaseResource(
            DB_SERVER_NAME=EnvVar("DB_SERVER_NAME"),
            DB_DATABASE_NAME=EnvVar("DB_DATABASE_NAME"),
            DB_USERNAME=EnvVar("DB_USERNAME"),
            DB_PASSWORD=EnvVar("DB_PASSWORD"),
            DB_PORT=EnvVar("DB_PORT")
        ),
        "airbyte": airbyte_workspace,
        "dbt_warehouse_resource": dbt_warehouse_resource
    }
)