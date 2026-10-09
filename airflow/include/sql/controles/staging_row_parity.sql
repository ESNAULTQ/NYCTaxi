-- STAGING conserve exactement le nombre de lignes du fichier mensuel RAW.
SELECT COUNT(*) = (
    SELECT COUNT(*)
    FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
    WHERE _source_file =
        'yellow_tripdata_{{ logical_date.strftime("%Y-%m") }}.parquet'
)
FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
WHERE source_file =
    'yellow_tripdata_{{ logical_date.strftime("%Y-%m") }}.parquet';
