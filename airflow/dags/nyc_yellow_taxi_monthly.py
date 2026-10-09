"""Chargement mensuel NYC Yellow Taxi : HTTP → stage Snowflake → RAW.

Une exécution traite le mois porté par sa date logique. Ne pas utiliser Trigger
sur ce DAG : son exécution manuelle chercherait le fichier du mois courant.
Le référentiel des zones est chargé une fois par le script Python hors Airflow.
"""

import tempfile
from pathlib import Path

import pendulum
import requests
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task
from airflow.utils.task_group import TaskGroup


SNOWFLAKE_CONNECTION_ID = "snowflake_nyc_taxi"
TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
STAGE = "NYC_TAXI.RAW.NYC_TAXI_STAGE"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"

# Les fichiers SQL fournis sont montés dans /usr/local/airflow/include/sql
# par Astro. Les chemins ci-dessous sont donc relatifs à ce répertoire.
SQL_TEMPLATE_PATH = "/usr/local/airflow/include/sql"


@dag(
    dag_id="nyc_yellow_taxi_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
    },
    template_searchpath=[SQL_TEMPLATE_PATH],
    params={
        "max_trip_distance_miles": 100,
        "max_trip_duration_min": 180,
        "max_rejected_pct": 25,
        "start_month": "2025-01-01",
        "end_month": "2025-04-01",
    },
    tags=["nyc_taxi", "ingestion", "raw"],
)
def nyc_yellow_taxi_monthly():
    """Build one idempotent ingestion and transformation chain per month.

    The ingestion tasks load exactly one monthly Parquet. The downstream SQL
    tasks are deliberately rerunnable: views/dimensions are replaced and the
    monthly fact/intermediate tables delete then reinsert the logical month.
    """

    @task
    def filename_from_logical_date() -> str:
        context = get_current_context()
        month = context["data_interval_start"].strftime("%Y-%m")
        return f"yellow_tripdata_{month}.parquet"

    @task
    def verify_file_is_published(filename: str) -> str:
        url = f"{TRIP_DATA_BASE_URL}/{filename}"
        response = requests.head(url, allow_redirects=True, timeout=30)
        response.raise_for_status()
        return url

    @task
    def download_and_put(url: str) -> str:
        """Download and PUT in one task because workers need not share /tmp."""
        filename = url.rsplit("/", 1)[-1]
        destination = Path(tempfile.gettempdir()) / filename

        try:
            with requests.get(url, stream=True, timeout=120) as response:
                response.raise_for_status()
                with destination.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            output.write(chunk)

            SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID).run(
                f"PUT file://{destination.resolve()} @{STAGE} "
                "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
            )
        finally:
            destination.unlink(missing_ok=True)

        return filename

    @task
    def copy_into_raw(filename: str) -> None:
        """Copy one Parquet into RAW; COPY history prevents duplicate rows."""
        SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID).run(
            f"""
            COPY INTO {TABLE}
            FROM @{STAGE}
            FILES = ('{filename}')
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
            """
        )

    filename = filename_from_logical_date()
    url = verify_file_is_published(filename)
    loaded_filename = download_and_put(url)
    raw_loaded = copy_into_raw(loaded_filename)

    # Chaque fichier SQL fourni correspond à une tâche identifiable. Les
    # dépendances suivent strictement les FROM/JOIN du SQL.
    with TaskGroup(group_id="setup") as setup:
        create_monthly_tables = SQLExecuteQueryOperator(
            task_id="create_monthly_tables",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="00_tables.sql",
            split_statements=True,
        )

    with TaskGroup(group_id="staging") as staging:
        stg_yellow_trips = SQLExecuteQueryOperator(
            task_id="stg_yellow_trips",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="staging/stg_tlc__yellow_trips.sql",
        )
        stg_taxi_zones = SQLExecuteQueryOperator(
            task_id="stg_taxi_zones",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="staging/stg_tlc__taxi_zones.sql",
        )
        codes_tlc = SQLExecuteQueryOperator(
            task_id="codes_tlc",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="staging/codes_tlc.sql",
        )

    with TaskGroup(group_id="intermediate") as intermediate:
        flagged = SQLExecuteQueryOperator(
            task_id="int_trips_flagged",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="intermediate/int_trips__flagged.sql",
            split_statements=True,
        )
        enriched = SQLExecuteQueryOperator(
            task_id="int_trips_enriched",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="intermediate/int_trips__enriched.sql",
            split_statements=True,
        )
        flagged >> enriched

    with TaskGroup(group_id="dimensions") as dimensions:
        dim_date = SQLExecuteQueryOperator(
            task_id="dim_date",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/dim_date.sql",
        )
        dim_payment = SQLExecuteQueryOperator(
            task_id="dim_payment_type",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/dim_payment_type.sql",
        )
        dim_rate = SQLExecuteQueryOperator(
            task_id="dim_rate_code",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/dim_rate_code.sql",
        )
        dim_vendor = SQLExecuteQueryOperator(
            task_id="dim_vendor",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/dim_vendor.sql",
        )
        dim_zone = SQLExecuteQueryOperator(
            task_id="dim_zone",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/dim_zone.sql",
        )

    with TaskGroup(group_id="facts_and_marts") as facts_and_marts:
        fact_trips = SQLExecuteQueryOperator(
            task_id="fct_trips",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/fct_trips.sql",
            split_statements=True,
        )
        daily_revenue = SQLExecuteQueryOperator(
            task_id="mart_daily_revenue",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/mart_daily_revenue.sql",
        )
        zone_hourly_demand = SQLExecuteQueryOperator(
            task_id="mart_zone_hourly_demand",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/mart_zone_hourly_demand.sql",
        )
        data_quality = SQLExecuteQueryOperator(
            task_id="mart_data_quality",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="marts/mart_data_quality.sql",
        )

        fact_trips >> [daily_revenue, zone_hourly_demand, data_quality]

    # Le contrôle fourni et trois contrôles complémentaires. SQLCheckOperator
    # échoue si la première valeur renvoyée est fausse; retries=0 rend l'échec
    # immédiatement visible dans l'interface.
    with TaskGroup(group_id="quality_checks") as quality_checks:
        raw_month_loaded = SQLCheckOperator(
            task_id="raw_month_loaded",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="controles/raw_mois_charge.sql",
            retries=0,
        )
        staging_row_parity = SQLCheckOperator(
            task_id="staging_row_parity",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="controles/staging_row_parity.sql",
            retries=0,
        )
        rejection_rate = SQLCheckOperator(
            task_id="rejection_rate_below_threshold",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="controles/rejection_rate.sql",
            retries=0,
        )
        enriched_keys_unique = SQLCheckOperator(
            task_id="enriched_trip_keys_unique",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="controles/enriched_keys_unique.sql",
            retries=0,
        )
        fact_month_present = SQLCheckOperator(
            task_id="fact_month_present",
            conn_id=SNOWFLAKE_CONNECTION_ID,
            sql="controles/fact_month_present.sql",
            retries=0,
        )

    raw_loaded >> raw_month_loaded
    raw_month_loaded >> setup
    setup >> staging
    stg_yellow_trips >> staging_row_parity
    staging_row_parity >> intermediate
    flagged >> rejection_rate
    rejection_rate >> enriched
    staging >> dimensions
    enriched >> enriched_keys_unique
    [enriched_keys_unique, dimensions] >> fact_trips
    facts_and_marts >> fact_month_present


nyc_yellow_taxi_monthly()
