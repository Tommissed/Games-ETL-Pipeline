from dagster import op, OpExecutionContext
from sqlalchemy import Table, MetaData, URL, create_engine
from sqlalchemy.dialects import postgresql
import math

from analytics.resources.postgresql import PostgresqlDatabaseResource 

#throws an error because metacritic can be null, so we must clean the data before inserting
def clean_value(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    if v == "NaT":
        return None
    return v

def upsert_to_database(
        postgres_conn: PostgresqlDatabaseResource,
        data: list[dict],
        table: Table,
        metadata: MetaData
    ) -> None:
    """Upserts data into the target database.

    Args:
        postgres_conn: a PostgresqlDatabaseResource object
        data: the transformed data
    """

    cleaned_data = [
        {k: clean_value(v) for k, v in row.items()}
        for row in data
    ]
    
    data = cleaned_data

    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=postgres_conn.DB_USERNAME,
        password=postgres_conn.DB_PASSWORD,
        host=postgres_conn.DB_SERVER_NAME,
        port=postgres_conn.DB_PORT,
        database=postgres_conn.DB_DATABASE_NAME
    )

    engine = create_engine(connection_url)
    metadata.create_all(engine)

    key_columns = [
        pk_column.name for pk_column in table.primary_key.columns.values()
    ]

    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={
            c.key: c for c in insert_statement.excluded if c.key not in key_columns
        },
    )
    with engine.begin() as connection:
        try:
            result = connection.execute(upsert_statement)
        except Exception as e:
            raise Exception(f"Failed to upsert to database, {e}")