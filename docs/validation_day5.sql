-- Validation finale Day 5 — requêtes en lecture seule sauf indication contraire.

USE ROLE NYC_TAXI_ENGINEER;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

-- 1. Comptages attendus après les trois mois.
SELECT 'RAW.YELLOW_TRIPDATA' AS object_name, COUNT(*) AS row_count
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
UNION ALL
SELECT 'RAW.TAXI_ZONE_LOOKUP', COUNT(*)
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
UNION ALL
SELECT 'INTERMEDIATE.INT_TRIPS__FLAGGED', COUNT(*)
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
UNION ALL
SELECT 'MARTS.FCT_TRIPS', COUNT(*)
FROM NYC_TAXI.MARTS.FCT_TRIPS
UNION ALL
SELECT 'MARTS.MART_ZONE_HOURLY_DEMAND', COUNT(*)
FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
UNION ALL
SELECT 'MARTS.MART_DATA_QUALITY', COUNT(*)
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY;

-- 2. Parité RAW/STAGING et unicité de la clé des faits.
SELECT
    (SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA)
      = (SELECT COUNT(*) FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS)
      AS raw_staging_row_parity,
    (SELECT COUNT(*) FROM NYC_TAXI.MARTS.FCT_TRIPS)
      = (SELECT COUNT(DISTINCT trip_sk) FROM NYC_TAXI.MARTS.FCT_TRIPS)
      AS fact_trip_sk_unique;

-- 3. Répartition qualité par mois et motif.
SELECT source_file, status, nb_rows, pct_of_file
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
ORDER BY source_file, status;

-- 4. Historique des chargements et preuve d'idempotence.
-- Le script de réparation a volontairement créé une seconde entrée historique
-- par fichier avec FORCE après TRUNCATE. La table RAW ne contient qu'une copie
-- correcte de chaque fichier ; les relances normales du DAG n'utilisent pas FORCE.
SELECT file_name, status, row_count, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'YELLOW_TRIPDATA',
    START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- 5. Grants à capturer avec le rôle d'administration humaine.
-- USE ROLE ACCOUNTADMIN;
-- SHOW GRANTS TO ROLE NYC_TAXI_ENGINEER;
-- SHOW GRANTS TO USER NYC_TAXI_AIRFLOW_SVC;

-- 6. Crédits : à exécuter avec ACCOUNTADMIN lorsque ACCOUNT_USAGE est disponible.
-- USE ROLE ACCOUNTADMIN;
-- SELECT warehouse_name, start_time, end_time, credits_used
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE warehouse_name = 'NYC_TAXI_WH'
--   AND start_time >= DATEADD(day, -14, CURRENT_TIMESTAMP())
-- ORDER BY start_time DESC;
