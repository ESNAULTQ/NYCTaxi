# Fiche source — NYC TLC Taxi Zone Lookup

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | NYC TLC Taxi Zone Lookup |
| Producteur des données | New York City Taxi & Limousine Commission (TLC) |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` |
| Accès | Public, sans authentification, HTTPS |
| Format du fichier | CSV avec en-tête et champs entre guillemets |
| Fréquence de publication | Référentiel ; aucune fréquence contractuelle n'est précisée |
| Délai entre période et publication | Non applicable : le fichier est chargé une fois lors de l'initialisation du projet |

## Volume mesuré

Mesure effectuée le 5 octobre 2026 sur `data/taxi_zone_lookup (1).csv`, copie locale du fichier TLC.

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---:|---:|---:|---|
| `taxi_zone_lookup (1).csv` | 12 331 octets — 12,04 KiB | 265 | 4 | DuckDB : `SELECT COUNT(*) FROM read_csv_auto('taxi_zone_lookup (1).csv', header=true)` |

## Colonnes

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `LocationID` | `BIGINT` | Identifiant TLC unique de la zone | `1` |
| `Borough` | `VARCHAR` | Borough ou catégorie géographique | `EWR` |
| `Zone` | `VARCHAR` | Nom de la zone TLC | `Newark Airport` |
| `service_zone` | `VARCHAR` | Catégorie de service TLC | `EWR` |

## Codes et valeurs observées

| Colonne | Valeur | Nombre de lignes | Interprétation |
|---|---|---:|---|
| `service_zone` | `Boro Zone` | 205 | Zone des boroughs |
| `service_zone` | `Yellow Zone` | 55 | Zone de service des taxis jaunes |
| `service_zone` | `Airports` | 2 | Zone aéroportuaire |
| `service_zone` | `EWR` | 1 | Newark Airport |
| `service_zone` | `N/A` | 2 | Valeur non applicable |

## Ce qui a surpris

- Les identifiants vont de 1 à 265, sans doublon : `COUNT(*) = COUNT(DISTINCT LocationID) = 265`.
- La zone `264` vaut `Borough = Unknown`, `Zone = N/A`, `service_zone = N/A`.
- La zone `265` vaut `Borough = N/A`, `Zone = Outside of NYC`, `service_zone = N/A`.
- Ces deux identifiants ne doivent pas être supprimés silencieusement : le SQL fourni les signale comme zones inconnues dans `DIM_ZONE`.

## Conséquences pour le pipeline

- Le CSV est chargé une seule fois avec le script Python, hors Airflow.
- La table RAW attend les colonnes renommées en minuscules : `locationid`, `borough`, `zone`, `service_zone`, `_source_file`, `_loaded_at`.
- Le CSV doit être normalisé sous le nom `taxi_zone_lookup.csv` avant le dépôt sur le stage ; les zones sont ensuite disponibles à chaque run pour `STG_TLC__TAXI_ZONES` et `DIM_ZONE`.
- La charge doit rester rejouable et conserver 265 lignes ainsi que 265 identifiants distincts.
