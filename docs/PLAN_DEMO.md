# Plan de démonstration — optionnel

Ce document est conservé comme support de lecture et de contrôle. Aucune
présentation orale n'est requise pour le rendu individuel.

## Avant la démonstration

- Démarrer Docker et Astro.
- Préparer Snowsight sur `docs/validation_day5.sql`.
- Préparer Airflow sur la vue Grid de `nyc_yellow_taxi_monthly`.
- Ouvrir `docs/REPONSE.md` et le schéma `docs/architecture.png`.
- Ne jamais ouvrir `.env`, `rsa_key.p8` ou une valeur secrète.
- Préparer les captures déjà collectées dans `doc/capture/` ; la capture 05
  est incluse dans `06_warehouse.png` avec les grants.

## Déroulé

| Temps | Sujet | Preuve |
|---:|---|---|
| 0:00–2:00 | Question métier et architecture médaillon | `architecture.png`, quatre couches |
| 2:00–4:00 | Sécurité Snowflake | rôle `NYC_TAXI_ENGINEER`, grants, compte de service |
| 4:00–6:30 | Orchestration | Grid/Graph Airflow, trois mois logiques |
| 6:30–9:00 | Idempotence | `COPY_HISTORY`, absence de doublons |
| 9:00–11:00 | Qualité bloquante | check rouge puis retour au vert |
| 11:00–13:30 | Réponse métier | top 10 de `REPONSE.md` et limites |
| 13:30–15:00 | Coûts et conclusion | warehouse XSMALL, auto-suspend, crédits |

## Questions à savoir expliquer

- Pourquoi la date logique Airflow est-elle utilisée pour le nom du fichier ?
- Pourquoi le CSV des zones est-il chargé hors du DAG mensuel ?
- Pourquoi RAW conserve-t-elle les lignes rejetées ?
- Comment `COPY INTO` et les tables mensuelles garantissent-ils l'idempotence ?
- Pourquoi `max_active_runs=1` ?
- Pourquoi le contrôle de qualité est-il placé avant ses consommateurs ?
- Pourquoi la requête finale joint-elle `FCT_TRIPS` à `DIM_PAYMENT_TYPE` ?
- Que faudrait-il modifier pour ajouter avril 2025 ?
