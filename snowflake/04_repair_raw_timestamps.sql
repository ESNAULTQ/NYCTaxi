-- Réparation unique du chargement Parquet TLC.
--
-- Pourquoi ce script existe : les trois premiers fichiers ont été chargés avec
-- le scanner Parquet historique. Les timestamps ont alors été interprétés avec
-- une mauvaise unité et produisent des années impossibles. Le fichier format
-- corrigé est défini dans 02_raw_objects.sql.
--
-- PRÉREQUIS : mettre le DAG Airflow en pause avant d'exécuter ce script.
-- Ce script vide et recharge uniquement RAW.YELLOW_TRIPDATA. Il ne touche pas
-- au référentiel RAW.TAXI_ZONE_LOOKUP.

USE ROLE NYC_TAXI_ENGINEER;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.PARQUET_FF
  TYPE = PARQUET
  USE_VECTORIZED_SCANNER = TRUE
  USE_LOGICAL_TYPE = TRUE
  COMMENT = 'Lecture des fichiers mensuels Yellow Taxi TLC avec timestamps logiques';

TRUNCATE TABLE NYC_TAXI.RAW.YELLOW_TRIPDATA;

-- FORCE est volontaire ici : Snowflake a déjà enregistré ces fichiers dans
-- COPY_HISTORY pendant le mauvais chargement.
COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
FROM @NYC_TAXI.RAW.NYC_TAXI_STAGE
FILES = (
    'yellow_tripdata_2025-01.parquet',
    'yellow_tripdata_2025-02.parquet',
    'yellow_tripdata_2025-03.parquet'
)
FILE_FORMAT = (FORMAT_NAME = NYC_TAXI.RAW.PARQUET_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (
    _source_file = METADATA$FILENAME,
    _loaded_at = METADATA$START_SCAN_TIME
)
FORCE = TRUE
ON_ERROR = ABORT_STATEMENT;

-- Contrôle immédiat : les dates doivent être réalistes, pas des années lointaines.
SELECT
    COUNT(*) AS nb_rows,
    MIN(tpep_pickup_datetime) AS min_pickup_at,
    MAX(tpep_pickup_datetime) AS max_pickup_at
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA;
