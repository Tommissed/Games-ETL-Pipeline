from dagster import ConfigurableResource

class PostgresqlDatabaseResource(ConfigurableResource):
    DB_SERVER_NAME: str
    DB_DATABASE_NAME: str
    DB_USERNAME: str
    DB_PASSWORD: str
    DB_PORT: str