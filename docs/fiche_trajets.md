# Fiche source — NYC TLC Yellow Taxi Trip Records

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | NYC TLC Yellow Taxi Trip Records |
| Producteur des données | New York City Taxi & Limousine Commission (TLC) |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet` — remplacer `01` par `02` ou `03` selon le mois |
| Accès | Public, sans authentification, HTTPS |
| Format du fichier | Apache Parquet |
| Fréquence de publication | Mensuelle, un fichier Yellow Taxi par mois |
| Délai entre période et publication | Aucun délai contractuel n'est indiqué par la source. Le pipeline vérifie donc la disponibilité HTTP du fichier avant de le télécharger. |
| Dictionnaire | `https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf` |

## Volume mesuré

Mesure effectuée le 5 octobre 2026 sur le fichier de janvier 2025, téléchargé depuis l'URL officielle.

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---:|---:|---:|---|
| `yellow_tripdata_2025-01.parquet` | 59 158 238 octets — 56,42 MiB | 3 475 226 | 20 | DuckDB : `SELECT COUNT(*) FROM read_parquet('yellow_tripdata_2025-01.parquet')` |

## Colonnes

Les types ci-dessous sont les types physiques lus dans le fichier de janvier. La table RAW utilise des types Snowflake larges afin d'accepter les variations entre mois.

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | `INTEGER` | Identifiant du fournisseur de technologie | `1` |
| `tpep_pickup_datetime` | `TIMESTAMP` | Date et heure de prise en charge | `2025-01-01 00:18:38` |
| `tpep_dropoff_datetime` | `TIMESTAMP` | Date et heure de dépose | `2025-01-01 00:26:59` |
| `passenger_count` | `BIGINT` | Nombre de passagers déclaré par le chauffeur | `1` |
| `trip_distance` | `DOUBLE` | Distance du trajet, en miles | `1.6` |
| `RatecodeID` | `BIGINT` | Code du tarif appliqué | `1` |
| `store_and_fwd_flag` | `VARCHAR` | Indique si le trajet a été mémorisé avant transmission | `N` |
| `PULocationID` | `INTEGER` | Identifiant TLC de la zone de prise en charge | `229` |
| `DOLocationID` | `INTEGER` | Identifiant TLC de la zone de dépose | `237` |
| `payment_type` | `BIGINT` | Code du mode de paiement | `1` |
| `fare_amount` | `DOUBLE` | Prix de base, en USD | `10.0` |
| `extra` | `DOUBLE` | Suppléments appliqués au tarif | `3.5` |
| `mta_tax` | `DOUBLE` | Taxe MTA, en USD | `0.5` |
| `tip_amount` | `DOUBLE` | Pourboire enregistré, en USD | `3.0` |
| `tolls_amount` | `DOUBLE` | Péages, en USD | `0.0` |
| `improvement_surcharge` | `DOUBLE` | Surcharge d'amélioration, en USD | `1.0` |
| `total_amount` | `DOUBLE` | Montant total facturé, en USD | `18.0` |
| `congestion_surcharge` | `DOUBLE` | Surcharge de congestion, en USD | `2.5` |
| `Airport_fee` | `DOUBLE` | Frais aéroport, en USD | `0.0` |
| `cbd_congestion_fee` | `DOUBLE` | Frais de congestion du quartier central, en USD | `0.0` |

## Codes

Les tables de codes sont matérialisées dans `STAGING` par le fichier fourni `codes_tlc.sql`.

### `VendorID`

| Valeur | Signification |
|---:|---|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix |

### `RatecodeID`

| Valeur | Signification |
|---:|---|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |
| 99 | Null or unknown |

### `payment_type`

| Valeur | Signification |
|---:|---|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

### `store_and_fwd_flag`

| Valeur | Signification |
|---|---|
| `Y` | Trajet temporairement stocké dans le véhicule avant transmission |
| `N` | Transmission sans ce stockage temporaire |

## Ce qui a surpris

Mesures réalisées avec DuckDB sur janvier 2025 :

- 90 893 trajets ont une distance nulle ou négative ; 144 118 ont un `fare_amount` négatif ; 63 596 ont un `total_amount` nul ou négatif.
- 2 051 trajets ont une date de dépose antérieure ou égale à la date de prise en charge.
- 22 prises en charge ne sont pas dans janvier : le minimum observé est le `2024-12-31 20:47:55` et le maximum le `2025-02-01 00:00:44`.
- 540 149 lignes ont `RatecodeID` et `store_and_fwd_flag` nuls ; ces valeurs doivent être préservées en RAW puis traitées selon les règles SQL fournies.
- Les pourboires ne sont interprétables que pour les paiements carte : les pourboires en espèces ne sont pas obligatoirement enregistrés.

## Conséquences pour le pipeline

- RAW conserve les 3 475 226 lignes sans filtrage et avec `_source_file` et `_loaded_at`.
- Les montants et distances doivent utiliser des types décimaux larges (`FLOAT` dans le contrat fourni), jamais un entier Snowflake sans échelle.
- STAGING ne doit pas changer le nombre de lignes.
- INTERMEDIATE attribue une seule raison de rejet par trajet et déduplique uniquement les trajets valides.
- Airflow déduit le mois du nom de fichier et de la date logique, pas de la date du jour.
