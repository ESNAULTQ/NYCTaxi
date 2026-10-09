"""Charge une fois le référentiel NYC TLC des zones dans RAW.

Exemple :
    python ingestion/load_zones.py
"""

import sys
import tempfile
from pathlib import Path

import requests

from snowflake_connection import connect_to_raw


ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
FILENAME = "taxi_zone_lookup.csv"
STAGE = "NYC_TAXI.RAW.NYC_TAXI_STAGE"
TABLE = "NYC_TAXI.RAW.TAXI_ZONE_LOOKUP"
FILE_FORMAT = "NYC_TAXI.RAW.CSV_FF"


def download(destination: Path) -> None:
    """Download the zone reference by chunks."""
    with requests.get(ZONES_URL, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)


def print_copy_result(cursor) -> None:
    """Print COPY INTO's file-level result without exposing any secret."""
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(columns, row)))


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="nyc_taxi_") as temporary_directory:
        local_file = Path(temporary_directory) / FILENAME
        print(f"Téléchargement : {ZONES_URL}")
        download(local_file)
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
                    FILES = ('{FILENAME}')
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


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Échec du chargement des zones : {error}", file=sys.stderr)
        raise SystemExit(1) from error
