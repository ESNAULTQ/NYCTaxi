"""Charge un mois Yellow Taxi TLC dans NYC_TAXI.RAW.YELLOW_TRIPDATA.

Exemple :
    python ingestion/load_month.py 2025-01
"""

import argparse
import re
import sys
import tempfile
from pathlib import Path

import requests

from snowflake_connection import connect_to_raw


TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
STAGE = "NYC_TAXI.RAW.NYC_TAXI_STAGE"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"
MONTH_PATTERN = re.compile(r"20\d{2}-(0[1-9]|1[0-2])")


def parse_month(value: str) -> str:
    """Validate a month argument and return it unchanged as YYYY-MM."""
    if not MONTH_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("Le mois doit être au format YYYY-MM, par exemple 2025-01.")
    return value


def download(url: str, destination: Path) -> None:
    """Download a public file by chunks so it is never held entirely in memory."""
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    output.write(chunk)


def print_copy_result(cursor) -> None:
    """Print COPY INTO's file-level result without exposing any secret."""
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(columns, row)))


def load_month(month: str) -> None:
    filename = f"yellow_tripdata_{month}.parquet"
    url = f"{TRIP_DATA_BASE_URL}/{filename}"

    with tempfile.TemporaryDirectory(prefix="nyc_taxi_") as temporary_directory:
        local_file = Path(temporary_directory) / filename
        print(f"Téléchargement : {url}")
        download(url, local_file)
        print(f"Fichier temporaire : {local_file.name} ({local_file.stat().st_size} octets)")

        connection = connect_to_raw()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"PUT file://{local_file.resolve()} @{STAGE} "
                    "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
                )
                print("Résultat PUT :")
                print_copy_result(cursor)

                cursor.execute(
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
                print("Résultat COPY INTO :")
                print_copy_result(cursor)
        finally:
            connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Charge un mois Yellow Taxi TLC dans RAW.")
    parser.add_argument("month", type=parse_month, help="Mois à charger, au format YYYY-MM")
    arguments = parser.parse_args()
    load_month(arguments.month)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Échec du chargement : {error}", file=sys.stderr)
        raise SystemExit(1) from error
