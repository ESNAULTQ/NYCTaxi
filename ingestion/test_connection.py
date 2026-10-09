"""Vérifie la connexion Snowflake par paire de clés du compte de service.

Usage :
    export SNOWFLAKE_ACCOUNT='ORGANISATION-COMPTE'
    export SNOWFLAKE_PRIVATE_KEY_PATH="$HOME/.ssh/snowflake/rsa_key.p8"
    python ingestion/test_connection.py

Ne jamais enregistrer une clé privée ou une valeur de variable d'environnement dans Git.
"""

import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


REQUIRED_ENVIRONMENT = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
)


def required_environment(name: str) -> str:
    """Return a required configuration value without displaying secret content."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement obligatoire absente : {name}")
    return value


def main() -> None:
    missing = [name for name in REQUIRED_ENVIRONMENT if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Variables manquantes : " + ", ".join(missing))

    private_key_path = Path(required_environment("SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser()
    if not private_key_path.is_file():
        raise RuntimeError(f"Clé privée introuvable : {private_key_path}")

    private_key = serialization.load_pem_private_key(
        private_key_path.read_bytes(),
        password=None,
    )

    connection = snowflake.connector.connect(
        account=required_environment("SNOWFLAKE_ACCOUNT"),
        user="NYC_TAXI_AIRFLOW_SVC",
        private_key=private_key,
        role="NYC_TAXI_ENGINEER",
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI",
        schema="RAW",
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()
                """
            )
            user, role, warehouse, database, schema = cursor.fetchone()
    finally:
        connection.close()

    print("Connexion réussie :")
    print(f"  user      = {user}")
    print(f"  role      = {role}")
    print(f"  warehouse = {warehouse}")
    print(f"  database  = {database}")
    print(f"  schema    = {schema}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Échec de connexion : {error}", file=sys.stderr)
        raise SystemExit(1) from error
