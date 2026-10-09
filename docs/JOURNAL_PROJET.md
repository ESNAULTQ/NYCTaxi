# NYC Yellow Taxi — Project Worklog (Days 1–5)

This document records what was completed during each project day, the evidence
obtained, the issues encountered, and the resulting decisions. It is intended
to make the project reproducible and to support the final presentation.

## Final architecture

```text
TLC monthly Parquet files ──Python/Airflow──> RAW
TLC zone CSV ───────────────Python once─────> RAW
                                             │
                                             ▼
                                          STAGING
                                             │
                                             ▼
                                       INTERMEDIATE
                                             │
                                             ▼
                                           MARTS
```

- Snowflake stores the four medallion layers.
- Python loads the monthly source files and the zone reference.
- Airflow orchestrates one monthly Parquet run using the logical date.
- The zone CSV is loaded once outside the monthly Airflow ingestion DAG.
- Airflow executes the SQL transformations and blocking quality checks.

---

## Day 1 — Source understanding and Snowflake infrastructure

### Objectives

- Understand the TLC sources and their structure.
- Measure a representative Parquet file and the zone reference.
- Create a secure Snowflake foundation.
- Prove that the service account can connect without `ACCOUNTADMIN`.

### Work completed

1. Inspected the project brief, starter kit, architecture diagram, and parcours.
2. Measured `yellow_tripdata_2025-01.parquet` with DuckDB:
   - 3,475,226 rows;
   - 20 columns;
   - 59,158,238 bytes;
   - timestamps, amounts, distances, codes, and anomalies documented.
3. Measured `taxi_zone_lookup.csv`:
   - 265 rows;
   - 265 distinct `LocationID` values;
   - 12,331 bytes.
4. Created the source documentation:
   - `docs/fiche_trajets.md`;
   - `docs/fiche_zones.md`.
5. Created Snowflake infrastructure:
   - warehouse `NYC_TAXI_WH`;
   - database `NYC_TAXI`;
   - schemas `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`;
   - role `NYC_TAXI_ENGINEER`;
   - service user `NYC_TAXI_AIRFLOW_SVC`.
6. Generated an RSA key pair and attached the public key to the service user.
7. Restricted the service account to the engineering role and project objects.

### Evidence

- Snowflake connection identity:

```text
user      = NYC_TAXI_AIRFLOW_SVC
role      = NYC_TAXI_ENGINEER
warehouse = NYC_TAXI_WH
database  = NYC_TAXI
schema    = RAW
```

- Warehouse configured as `XSMALL`, auto-suspend 60 seconds, auto-resume enabled.
- Private key remains outside the project and is not committed.

### Issues and resolutions

- The first service-user creation attempt used `USERADMIN` and lacked account
  authorization. The bootstrap section was corrected to use `ACCOUNTADMIN` only
  for the one-time account object creation; the service user never receives
  `ACCOUNTADMIN`.
- Python virtualenv creation was unavailable through the system package,
  so a local `uv` environment was used instead.

### Main files

- `snowflake/01_infrastructure.sql`
- `snowflake/02_set_service_public_key.sql.example`
- `ingestion/requirements.txt`
- `ingestion/snowflake_connection.py`
- `ingestion/test_connection.py`

---

## Day 2 — RAW ingestion and idempotence

### Objectives

- Create the file formats, stage, and RAW tables.
- Load the zones and monthly trips.
- Preserve source metadata and decimal values.
- Prove that a repeated load does not duplicate rows.

### Work completed

1. Created the Parquet and CSV file formats.
2. Created the internal stage `NYC_TAXI.RAW.NYC_TAXI_STAGE`.
3. Created:
   - `RAW.YELLOW_TRIPDATA`;
   - `RAW.TAXI_ZONE_LOOKUP`.
4. Implemented Python loaders:
   - `ingestion/load_month.py`;
   - `ingestion/load_zones.py`.
5. Loaded the zone CSV once, outside Airflow:
   - 265 rows;
   - 0 errors.
6. Loaded January 2025:
   - 3,475,226 rows;
   - 0 errors.
7. Re-ran the January load and confirmed that Snowflake did not add duplicate
   rows.

### Evidence

```text
taxi_zone_lookup.csv             265 rows loaded
yellow_tripdata_2025-01.parquet  3,475,226 rows loaded
```

`COPY_HISTORY` showed one successful normal load for the January file before the
later timestamp repair.

### Main files

- `snowflake/02_raw_objects.sql`
- `snowflake/03_raw_verifications.sql`
- `ingestion/load_month.py`
- `ingestion/load_zones.py`

---

## Day 3 — Airflow and monthly orchestration

### Objectives

- Start a local Airflow 3 project with Astro.
- Configure key-pair authentication to Snowflake.
- Orchestrate January, February, and March from logical dates.
- Keep the monthly pipeline idempotent.

### Work completed

1. Initialized the Astro project in `airflow/`.
2. Installed the Snowflake provider in `airflow/requirements.txt`.
3. Created the connection `snowflake_nyc_taxi`.
4. Created and successfully ran `test_snowflake_connection`.
5. Created `nyc_yellow_taxi_monthly` with these tasks:

```text
filename_from_logical_date
→ verify_file_is_published
→ download_and_put
→ copy_into_raw
```

6. Configured monthly scheduling, catchup, and `max_active_runs=1`.
7. Successfully loaded:
   - January: 3,475,226 rows;
   - February: 3,577,543 rows;
   - March: 4,145,257 rows.

Total RAW volume:

```text
11,198,026 rows
```

### Issues and resolutions

- Docker initially rejected the current user because the Docker group session
  had not been refreshed. `newgrp docker` and a terminal restart resolved it.
- The Airflow Snowflake provider rejected raw PEM text in the UI. The provider
  requires `private_key_content` to be Base64 encoded, so the connection was
  corrected with:

```bash
base64 -w 0 /home/simplon/.ssh/snowflake/rsa_key.p8
```

### Main files

- `airflow/dags/test_snowflake_connection.py`
- `airflow/dags/nyc_yellow_taxi_monthly.py`
- `airflow/.env.example`
- `airflow/requirements.txt`

---

## Day 4 — Transformations and data-quality controls

### Objectives

- Execute the supplied SQL in dependency order.
- Group transformations by medallion layer.
- Add blocking quality checks.
- Demonstrate a controlled failure and recovery.

### Work completed

The monthly DAG was extended with these TaskGroups:

```text
setup
→ staging
→ intermediate
→ dimensions
→ facts_and_marts
→ quality_checks
```

The DAG now executes one Airflow task per SQL file and uses the run's logical
month for all monthly filters.

Quality checks implemented:

- `raw_month_loaded`;
- `staging_row_parity`;
- `rejection_rate_below_threshold`;
- `enriched_trip_keys_unique`;
- `fact_month_present`.

### Critical issue discovered

The first transformation runs loaded Parquet timestamps into years such as
`55006149`. Consequently, every trip was classified as `duration_too_long` and
the rejection rate was 100%.

The root cause was the legacy Parquet scanner interpreting the TLC logical
nanosecond timestamps with the wrong unit.

### Resolution

The Parquet format was corrected to:

```sql
USE_VECTORIZED_SCANNER = TRUE
USE_LOGICAL_TYPE = TRUE
```

The one-time repair script then:

1. paused the DAG;
2. truncated only `RAW.YELLOW_TRIPDATA`;
3. reloaded the three staged files with `FORCE = TRUE`;
4. verified realistic timestamps;
5. reran the transformations.

The two isolated 2007/2009 timestamps and out-of-month timestamps are treated
as data-quality anomalies by the documented rejection rules.

### Final evidence

```text
RAW.YELLOW_TRIPDATA              11,198,026
INTERMEDIATE.INT_TRIPS__FLAGGED  11,198,026
MARTS.FCT_TRIPS                  10,382,378
MARTS.MART_ZONE_HOURLY_DEMAND    11,524
MARTS.MART_DATA_QUALITY          18
```

The rejection threshold was temporarily made intentionally too strict. The
quality task failed, downstream tasks were blocked, the threshold was restored,
and the run returned to green. This proves the checks are genuinely blocking.

### Main files

- `airflow/dags/nyc_yellow_taxi_monthly.py`
- `airflow/include/sql/00_tables.sql`
- `airflow/include/sql/staging/`
- `airflow/include/sql/intermediate/`
- `airflow/include/sql/marts/`
- `airflow/include/sql/controles/`
- `snowflake/04_repair_raw_timestamps.sql`

---

## Day 5 — Business answer, evidence, and handoff

### Objectives

- Produce the answer to the business question.
- Consolidate final validation queries.
- Prepare the demonstration and delivery evidence.

### Work completed

1. Joined `FCT_TRIPS` to `DIM_ZONE` and `DIM_PAYMENT_TYPE` so that the final
   answer includes zone, hour, day type, and payment method.
2. Executed the final aggregation and recorded the top 10 combinations.
3. Identified the leading result:

```text
Midtown Center — weekday, 18:00, credit card
32,496 trips; $829,863.82 total revenue; $25.54 average revenue per trip
```

4. Documented the quality breakdown by source file and rejection reason.
5. Prepared read-only validation queries for counts, parity, uniqueness,
   quality, and `COPY_HISTORY`.
6. Prepared the grants and warehouse-credit queries for the final evidence.
7. Prepared a 15-minute presentation plan.
8. Audited project documentation for private-key material; none was found.

### Main deliverables

- `docs/REPONSE.md`
- `docs/validation_day5.sql`
- `docs/PLAN_DEMO.md`
- `README.md`
- `doc/GUIDE_DIRECTEUR_PROJET_NYC_TAXI.md`

---

## Final competency evidence

| Competency | Evidence in this project |
|---|---|
| C2 — Map data | Source sheets, measured volumes, columns, codes, anomalies |
| C3 — Design the technical framework | Architecture, four layers, tool responsibilities |
| C8 — Automate extraction | Python monthly loader, logical dates, catchup, idempotence |
| C9 — SQL queries | Transformations, final business query, blocking controls |
| C14 — Create the warehouse | Snowflake infrastructure, RAW contract, schemas, types |
| C15 — Integrate ETL | Airflow TaskGroups, dependency graph, three monthly runs |
| C16 — Manage the warehouse | Least privilege, key pair, no secrets in code, cost controls |

## Evidence inventory at handoff

The Snowflake validation captures are stored in `doc/capture/`:

- `01_final_counts.png` — final object counts;
- `02_data_integrity.png` — parity and uniqueness;
- `03_data_quality.png` — quality breakdown;
- `04_copy_history.png` — load history and idempotence;
- `06_warehouse.png` — warehouse configuration and the grants evidence kept
  with it instead of a separate capture 05;
- `07_access_denied.png` — refused out-of-scope access;
- `08_credits.png` — warehouse credit usage.

The remaining optional presentation evidence is a dedicated Airflow Grid/Graph
capture and a clean-clone reproduction log. These do not change the validated
Snowflake counts or the successful DAG result.
