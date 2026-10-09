-- Une exécution mensuelle doit produire des faits consommables.
SELECT COUNT(*) > 0
FROM NYC_TAXI.MARTS.FCT_TRIPS
WHERE source_file_month = '{{ ds }}'::date;
