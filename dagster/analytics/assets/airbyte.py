from dagster import EnvVar, AutomationCondition, AssetSpec, AssetKey
from dagster_airbyte import AirbyteWorkspace, build_airbyte_assets_definitions, DagsterAirbyteTranslator, AirbyteConnectionTableProps # type: ignore

# https://docs.dagster.io/api/libraries/dagster-airbyte

class CustomDagsterAirbyteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props: AirbyteConnectionTableProps) -> AssetSpec:
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            key=AssetKey(["rawg", props.table_name]),
            group_name="airbyte_assets",
            automation_condition=AutomationCondition.on_cron(cron_schedule="* * * * *") # runs every minute
        )

airbyte_workspace = AirbyteWorkspace(
    rest_api_base_url="http://localhost:8000/api/public/v1",
    configuration_api_base_url="http://localhost:8000/api/v1",
    workspace_id=EnvVar("AIRBYTE_WORKSPACE_ID"),
    client_id=EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=EnvVar("AIRBYTE_CLIENT_SECRET"),
)

all_airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomDagsterAirbyteTranslator()
)