# Réponse à la direction d’Hudson Cab Partners

> Livrable technique — contributeur identifié : `ESNAULTQ`.
> Ajouter le second membre du binôme avant le rendu final.

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et
combien rapporte un trajet selon la zone, l’heure et le mode de paiement ?

## Périmètre analysé

- Période : janvier à mars 2025.
- RAW : 11 198 026 trajets.
- Faits analytiques : 10 382 378 trajets valides et dédoublonnés.
- Les lignes invalides reçoivent une seule raison de rejet, selon l’ordre défini
  dans `INTERMEDIATE.INT_TRIPS__FLAGGED`.
- Les zones et les modes de paiement sont enrichis par les dimensions Snowflake.

## La requête

La table `MART_ZONE_HOURLY_DEMAND` répond à la demande par zone et heure, mais
ne contient pas le mode de paiement. La réponse finale joint donc les faits aux
dimensions de zone et de paiement.

```sql
SELECT
    z.zone_name AS pickup_zone,
    f.pickup_hour,
    IFF(f.is_weekend, 'Weekend', 'Weekday') AS day_type,
    COALESCE(p.payment_type_label, 'Unknown') AS payment_type,
    COUNT(*) AS nb_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance_miles,
    ROUND(AVG(f.trip_duration_min), 1) AS avg_duration_min
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE z
    ON z.zone_key = f.pickup_zone_key
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p
    ON p.payment_type_key = f.payment_type_key
GROUP BY 1, 2, 3, 4
ORDER BY nb_trips DESC, total_revenue DESC
LIMIT 10;
```

## Le résultat : les 10 premières lignes

| Zone de prise en charge | Heure | Type de jour | Paiement | Trajets | Revenu total ($) | Revenu moyen ($) | Distance moyenne (mi) | Durée moyenne (min) |
|---|---:|---|---|---:|---:|---:|---:|---:|
| Midtown Center | 18 | Weekday | Credit card | 32 496 | 829 863,82 | 25,54 | 1,95 | 14,0 |
| Midtown Center | 17 | Weekday | Credit card | 31 563 | 854 001,08 | 27,06 | 2,04 | 15,7 |
| Midtown Center | 19 | Weekday | Credit card | 27 417 | 677 262,40 | 24,70 | 2,03 | 12,7 |
| Midtown Center | 20 | Weekday | Credit card | 24 724 | 588 466,94 | 23,80 | 2,26 | 12,5 |
| Midtown Center | 16 | Weekday | Credit card | 24 255 | 663 294,81 | 27,35 | 2,16 | 15,6 |
| Midtown East | 18 | Weekday | Credit card | 24 185 | 611 993,88 | 25,30 | 1,99 | 13,5 |
| Upper East Side North | 15 | Weekday | Credit card | 23 754 | 489 379,95 | 20,60 | 1,67 | 12,8 |
| Upper East Side South | 18 | Weekday | Credit card | 23 549 | 520 073,43 | 22,08 | 1,64 | 10,9 |
| Upper East Side South | 17 | Weekday | Credit card | 23 352 | 526 744,53 | 22,56 | 1,63 | 11,6 |
| Upper East Side South | 14 | Weekday | Credit card | 22 868 | 470 528,15 | 20,58 | 1,61 | 12,5 |

## Ce qu’il faut en retenir

1. La demande la plus forte se concentre dans Midtown Center pendant les jours
   ouvrés, surtout entre 16 h et 20 h ; le maximum observé est à 18 h avec
   32 496 trajets valides payés par carte.
2. Midtown Center, Midtown East et Upper East Side South dominent le top 10.
   Les trajets de ces créneaux rapportent en moyenne environ 20,58 à 27,35 $.
3. Le plus grand volume ne correspond pas toujours au plus grand revenu moyen :
   la ligne de 17 h à Midtown Center rapporte davantage au total que celle de
   18 h, car son revenu moyen est supérieur.

## Qualité et anomalies

| Fichier | Trajets valides | Rejets principaux |
|---|---:|---|
| Janvier 2025 | 3 251 337 (93,558 %) | Montant non positif : 130 112 ; distance hors plage : 90 327 |
| Février 2025 | 3 305 246 (92,389 %) | Montant non positif : 166 569 ; distance hors plage : 99 232 |
| Mars 2025 | 3 825 795 (92,293 %) | Montant non positif : 192 454 ; distance hors plage : 103 121 |

Les comptages de `MART_DATA_QUALITY` sont exclusifs : un trajet reçoit la
première règle de rejet rencontrée, et non toutes les anomalies qu’il pourrait
présenter.

## Les limites

- L’analyse couvre seulement janvier–mars 2025 et ne permet pas de conclure
  sur une saison complète ni sur une évolution annuelle.
- Les trajets invalides ou dédoublonnés ne sont pas dans `FCT_TRIPS`.
- La TLC ne fournit pas d’identifiant de trajet ; `trip_sk` repose donc sur une
  clé métier composite et la déduplication est un compromis documenté.
- Les zones 264 et 265 sont conservées comme zones inconnues/hors NYC.
- Les pourboires ne sont interprétables que pour les paiements par carte ; un
  pourboire en espèces peut ne pas être enregistré.
- Ces résultats décrivent des volumes observés et ne prouvent pas une relation
  causale entre zone, heure, paiement et revenu.

## Reproduction

1. Démarrer Docker et Astro depuis `airflow/` avec `astro dev start`.
2. Vérifier la connexion `snowflake_nyc_taxi` dans Airflow.
3. Activer `nyc_yellow_taxi_monthly` et utiliser les exécutions planifiées
   janvier, février et mars 2025.
4. Exécuter la requête ci-dessus avec le rôle `NYC_TAXI_ENGINEER`.
