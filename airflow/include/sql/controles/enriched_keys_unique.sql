-- La déduplication garantit une clé métier unique dans la couche enrichie.
SELECT COUNT(*) = COUNT(DISTINCT trip_sk)
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__ENRICHED
WHERE source_file_month = '{{ ds }}'::date;
