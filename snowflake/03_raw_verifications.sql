-- Vérifications de la couche RAW après chargement.

USE ROLE NYC_TAXI_ENGINEER;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

-- Janvier doit compter exactement 3 475 226 lignes après le premier chargement.
SELECT
    _source_file,
    COUNT(*) AS nb_rows,
    MIN(_loaded_at) AS first_loaded_at,
    MAX(_loaded_at) AS last_loaded_at
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
GROUP BY 1
ORDER BY 1;

-- Le référentiel ne contient qu'une ligne par zone.
SELECT
    COUNT(*) AS nb_zones,
    COUNT(DISTINCT locationid) AS nb_location_ids
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP;

-- Les montants doivent conserver leurs décimales.
SELECT fare_amount, total_amount
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
LIMIT 10;

-- Preuve de l'idempotence : le même fichier ne doit être chargé qu'une fois.
SELECT file_name, status, row_count, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'YELLOW_TRIPDATA',
    START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
