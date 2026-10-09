"""Configuration de connexion Snowflake partagée par les scripts d'ingestion.

La clé privée est lue depuis un chemin local défini par une variable d'environnement.
Elle n'est jamais copiée dans le projet, les logs ou une image Docker.
"""

import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization


DEFAULT_USER = "NYC_TAXI_AIRFLOW_SVC"
DEFAULT_ROLE = "NYC_TAXI_ENGINEER"
DEFAULT_WAREHOUSE = "NYC_TAXI_WH"
DEFAULT_DATABASE = "NYC_TAXI"
DEFAULT_SCHEMA = "RAW"


def required_environment(name: str) -> str:
    """Return a required configuration value without exposing secrets."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement obligatoire absente : {name}")
    return value


def connect_to_raw():
    """Open a key-pair authenticated connection using the restricted tool role."""
    private_key_path = Path(required_environment("SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser()
    if not private_key_path.is_file():
        raise RuntimeError(f"Clé privée introuvable : {private_key_path}")

    private_key = serialization.load_pem_private_key(
        private_key_path.read_bytes(),
        password=None,
    )

    return snowflake.connector.connect(
        account=required_environment("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER", DEFAULT_USER),
        private_key=private_key,
        role=os.environ.get("SNOWFLAKE_ROLE", DEFAULT_ROLE),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
        database=os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_DATABASE),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", DEFAULT_SCHEMA),
    )
