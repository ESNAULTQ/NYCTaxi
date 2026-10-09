"""DAG manuel minimal pour valider la connexion Airflow → Snowflake.

Lancer ce DAG uniquement depuis le bouton Trigger : il ne traite aucune période.
Le log doit afficher NYC_TAXI_AIRFLOW_SVC, NYC_TAXI_ENGINEER et NYC_TAXI_WH.
"""

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task


SNOWFLAKE_CONNECTION_ID = "snowflake_nyc_taxi"


@dag(
    dag_id="test_snowflake_connection",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nyc_taxi", "setup"],
)
def test_snowflake_connection():
    """Print the authenticated service identity without exposing the private key."""

    @task
    def who_am_i():
        row = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID).get_first(
            """
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
            """
        )
        print(f"Snowflake connection identity: {row}")

    who_am_i()


test_snowflake_connection()
