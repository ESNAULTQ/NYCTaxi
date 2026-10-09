-- Couche RAW — objets de chargement NYC Yellow Taxi
-- À exécuter avec Run All dans Snowsight.
-- Les objets sont créés avec le rôle des outils : il en devient propriétaire.

USE ROLE NYC_TAXI_ENGINEER;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

-- Formats de fichier réutilisables.
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.PARQUET_FF
  TYPE = PARQUET
  -- Les fichiers TLC utilisent des timestamps Parquet logiques en nanosecondes.
  -- Le scanner historique (valeur par défaut FALSE) les interprète avec une
  -- mauvaise unité et produit des années impossibles (ex. 55 millions).
  USE_VECTORIZED_SCANNER = TRUE
  USE_LOGICAL_TYPE = TRUE
  COMMENT = 'Lecture des fichiers mensuels Yellow Taxi TLC';

CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.CSV_FF
  TYPE = CSV
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Lecture du référentiel TLC des zones';

-- Les fichiers sont déposés à la racine de ce stage afin que _source_file
-- conserve exactement yellow_tripdata_YYYY-MM.parquet.
CREATE STAGE IF NOT EXISTS NYC_TAXI.RAW.NYC_TAXI_STAGE
  COMMENT = 'Dépôt interne des fichiers TLC avant COPY INTO';

-- Contrat RAW : noms et colonnes imposés par les SQL fournis.
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.YELLOW_TRIPDATA (
    vendorid                INTEGER,
    tpep_pickup_datetime    TIMESTAMP_NTZ,
    tpep_dropoff_datetime   TIMESTAMP_NTZ,
    passenger_count         INTEGER,
    trip_distance           FLOAT,
    ratecodeid              INTEGER,
    store_and_fwd_flag      VARCHAR,
    pulocationid            INTEGER,
    dolocationid            INTEGER,
    payment_type            INTEGER,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    airport_fee             FLOAT,
    cbd_congestion_fee      FLOAT,
    _source_file            VARCHAR,
    _loaded_at              TIMESTAMP_NTZ
)
COMMENT = 'Copie fidèle des Parquet mensuels Yellow Taxi TLC, avec traçabilité';

CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.TAXI_ZONE_LOOKUP (
    locationid              INTEGER,
    borough                 VARCHAR,
    zone                    VARCHAR,
    service_zone            VARCHAR,
    _source_file            VARCHAR,
    _loaded_at              TIMESTAMP_NTZ
)
COMMENT = 'Référentiel TLC des 265 zones de taxi, avec traçabilité';

-- Vérifications immédiates : le propriétaire doit être NYC_TAXI_ENGINEER.
SHOW FILE FORMATS IN SCHEMA NYC_TAXI.RAW;
SHOW STAGES IN SCHEMA NYC_TAXI.RAW;
SHOW TABLES IN SCHEMA NYC_TAXI.RAW;
