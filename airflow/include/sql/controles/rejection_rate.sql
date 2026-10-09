-- Le taux de rejets du mois reste sous le seuil de qualité du DAG.
SELECT COALESCE(
    100 * COUNT_IF(rejection_reason IS NOT NULL) / NULLIF(COUNT(*), 0), 0
) <= {{ params.max_rejected_pct }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
