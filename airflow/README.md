# Airflow local — NYC Yellow Taxi

Ce dossier contient le projet Astro/Airflow qui orchestre le chargement mensuel
des fichiers Yellow Taxi et les transformations Snowflake.

## Contenu utile

- `dags/nyc_yellow_taxi_monthly.py` : DAG mensuel, ingestion, transformations
  et contrôles qualité bloquants ;
- `dags/test_snowflake_connection.py` : test de l'identité Snowflake ;
- `include/sql/` : scripts SQL de staging, intermediate, dimensions, faits,
  marts et contrôles ;
- `requirements.txt` : dépendances Python ;
- `.env.example` : modèle local de configuration, sans secret réel.

Le DAG mensuel utilise la connexion Airflow `snowflake_nyc_taxi` et le compte de
service Snowflake `NYC_TAXI_AIRFLOW_SVC` avec le rôle `NYC_TAXI_ENGINEER`.

## Démarrage

Prérequis : Docker Desktop démarré, la clé privée Snowflake conservée hors du
dépôt et la connexion Airflow configurée dans l'interface.

```bash
cd /home/simplon/projects/NYCTaxi/airflow
astro dev start
```

Ouvrir ensuite <http://airflow.localhost:6563>. Si l'interface ne s'ouvre pas
automatiquement, saisir cette adresse dans le navigateur. Vérifier d'abord que
les conteneurs sont actifs avec `docker ps`.

Pour arrêter l'environnement :

```bash
astro dev stop
```

## Utilisation du DAG

1. Dans **DAGs**, activer `nyc_yellow_taxi_monthly` avec son interrupteur.
2. Utiliser les exécutions planifiées pour janvier, février et mars 2025.
3. Pour rejouer une tâche d'une exécution déjà terminée, sélectionner les
   tâches puis utiliser **Clear**.
4. Pour tester le contrôle bloquant, modifier temporairement le seuil prévu
   dans le DAG, lancer une exécution contrôlée, vérifier l'échec et restaurer
   immédiatement la valeur avant de relancer.

Éviter **Trigger DAG** pour un mois historique sans définir explicitement la
date logique : le bouton crée par défaut une exécution datée du jour courant.
Le DAG est séquentiel (`max_active_runs=1`) afin d'éviter les remplacements
concurrents des tables mensuelles.

## Vérifications avant livraison

- `test_snowflake_connection` renvoie l'utilisateur de service, le rôle
  `NYC_TAXI_ENGINEER`, le warehouse `NYC_TAXI_WH`, la base `NYC_TAXI` et le
  schéma `RAW` ;
- les trois exécutions mensuelles sont vertes ;
- les contrôles `raw_month_loaded`, `staging_row_parity`,
  `rejection_rate_below_threshold`, `enriched_trip_keys_unique` et
  `fact_month_present` sont verts ;
- aucun `.env`, clé privée ou mot de passe n'est ajouté à Git.

Les résultats et les requêtes de preuve sont regroupés dans
`../docs/validation_day5.sql` et `../docs/ETAT_REEL_PROJET.md`.
