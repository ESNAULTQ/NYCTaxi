# Actual Project Status — NYC Yellow Taxi

Status based on the work actually executed in this workspace and the results
you reported in Snowflake and Airflow. This document deliberately separates
completed work from work that is only prepared or still needs proof.

## Executive summary

### Completed and evidenced

- Snowflake infrastructure exists and the service account connects with the
  restricted role `NYC_TAXI_ENGINEER`.
- The zone reference contains 265 rows and 265 distinct zones.
- RAW contains January, February, and March 2025: 11,198,026 rows total.
- The Airflow connection test succeeded.
- The monthly Airflow DAG ran for January, February, and March.
- The transformation result reached the expected counts:

```text
INT_TRIPS__FLAGGED       11,198,026
FCT_TRIPS                10,382,378
MART_ZONE_HOURLY_DEMAND  11,524
MART_DATA_QUALITY        18
```

- The timestamp-loading problem was diagnosed and repaired.
- The quality-check failure and return-to-green demonstration was reported as
  completed.
- The final business aggregation was executed and its top 10 results were
  recorded in `docs/REPONSE.md`.

### Prepared but not yet evidenced as completed

- Account-level grant screenshots.
- A deliberately refused out-of-scope Snowflake action.
- Warehouse credit usage from `SNOWFLAKE.ACCOUNT_USAGE`.
- The complete capture package in `docs/captures/`.
- A clean clone/reinstall reproduction test.
- A timed rehearsal of the 15-minute presentation.

The last group is not a pipeline failure; it is final delivery evidence that
still has to be collected.

---

## Day 1 — What was actually done

### Completed

- Read and analyzed the brief and pipeline architecture.
- Measured January 2025 with DuckDB:
  - 3,475,226 rows;
  - 20 columns;
  - 59,158,238 bytes.
- Measured the zone CSV:
  - 265 rows;
  - 265 distinct `LocationID` values;
  - 12,331 bytes.
- Created source sheets for trips and zones.
- Created the Snowflake warehouse, database, four schemas, engineering role,
  and service user.
- Generated the RSA pair and configured the public key.
- Passed the Python service-account connection test.

### Problems resolved

- The service-user creation initially failed under `USERADMIN`; the bootstrap
  script was corrected and the user was then created successfully.
- The local Python environment was created with `uv` because the system Python
  virtualenv package was unavailable.

### Not done or not independently evidenced

- No formal access-denied capture was produced at this stage.
- No credit-usage measurement was performed on Day 1.

---

## Day 2 — What was actually done

### Completed

- Created the RAW file formats, stage, and tables.
- Loaded `taxi_zone_lookup.csv`: 265 rows, no errors.
- Loaded January 2025: 3,475,226 rows, no errors.
- Re-ran the January load and confirmed no duplicate rows were added.
- Verified `COPY_HISTORY`, source filenames, load timestamps, and decimal
  values.

### Not done or not independently evidenced

- No external object-storage stage was created; the project uses a Snowflake
  internal stage as designed.
- The zone CSV was not loaded by Airflow; it was intentionally loaded once with
  Python outside the monthly DAG.

---

## Day 3 — What was actually done

### Completed

- Initialized the Astro project.
- Started Airflow locally with Docker Desktop.
- Created the Airflow Snowflake connection `snowflake_nyc_taxi`.
- Corrected the UI private-key format to Base64.
- Ran `test_snowflake_connection` successfully.
- Ran the monthly DAG for January, February, and March 2025.
- Loaded the three monthly RAW files:

```text
January   3,475,226
February  3,577,543
March     4,145,257
```

### Problems resolved

- Docker group permissions were refreshed with `newgrp docker`.
- The Airflow Snowflake provider error `Incorrect padding` was resolved by
  Base64-encoding `private_key_content`.

### Not done or not independently evidenced

- No hosted Astro deployment was performed; Airflow is local only.
- No alerting or Discord notification was implemented.

---

## Day 4 — What was actually done

### Completed

- Added Airflow TaskGroups for setup, STAGING, INTERMEDIATE, dimensions, marts,
  and quality checks.
- Added five quality checks:
  - RAW month present;
  - RAW/STAGING row parity;
  - rejection rate threshold;
  - unique enriched trip key;
  - fact month present.
- Initially observed failed checks because timestamps had been loaded as years in
  the millions.
- Confirmed the actual cause: the legacy Parquet scanner used the wrong unit for
  logical nanosecond timestamps.
- Added `USE_VECTORIZED_SCANNER = TRUE` and `USE_LOGICAL_TYPE = TRUE`.
- Ran the one-time RAW repair with `FORCE = TRUE` after truncating the corrupted
  trip table.
- Reran the transformations and reached the expected final counts.
- Corrected the DAG dependencies so quality checks block downstream tasks.
- Performed the intentional failure and reported the return to green.

### Important interpretation

The `FORCE = TRUE` repair is exceptional. It created additional entries in
`COPY_HISTORY`, but the table itself contains one corrected copy of each file
because it was truncated before the repair. Normal DAG runs do not use `FORCE`.

### Not done or not independently evidenced

- The final green state was reported from the Airflow UI, but no screenshot has
  yet been stored in the project capture folder.

---

## Day 5 — What was actually done

### Completed

- Executed the final read-only business aggregation in Snowflake.
- Recorded the top 10 zone/hour/day/payment combinations.
- Recorded the quality breakdown from `MART_DATA_QUALITY`.
- Created:
  - `docs/REPONSE.md`;
  - `docs/validation_day5.sql`;
  - `docs/PLAN_DEMO.md`;
  - this actual-status document.
- Ran Python static compilation for the ingestion scripts and DAG files.
- Ran Astro DAG parsing successfully with no import errors.
- Searched project documentation and code for private-key material; none was
  found.

### Only prepared, not yet executed by the user

- `docs/validation_day5.sql` has been written, but its grants and credit-query
  sections still need to be run in Snowsight.
- The final documentation exists, but author names and screenshots still need
  to be added.
- The access-denied test still needs to be executed and captured.
- A clean-clone reproduction has not been performed.
- The final presentation has not yet been timed and rehearsed.

---

## Current file inventory

### Implemented pipeline

- `snowflake/01_infrastructure.sql`
- `snowflake/02_raw_objects.sql`
- `snowflake/03_raw_verifications.sql`
- `snowflake/04_repair_raw_timestamps.sql`
- `ingestion/load_month.py`
- `ingestion/load_zones.py`
- `ingestion/test_connection.py`
- `airflow/dags/test_snowflake_connection.py`
- `airflow/dags/nyc_yellow_taxi_monthly.py`

### Final documentation

- `docs/JOURNAL_PROJET.md` — detailed chronological worklog;
- `docs/ETAT_REEL_PROJET.md` — factual completed/pending status;
- `docs/REPONSE.md` — business answer;
- `docs/validation_day5.sql` — validation and evidence queries;
- `docs/PLAN_DEMO.md` — presentation plan.

## Honest next actions

The core technical pipeline is operational. The remaining work is evidence and
delivery, not a new transformation layer:

1. Run the final validation, grants, and credit queries.
2. Execute and capture the refused-access test.
3. Add the required screenshots under `docs/captures/`.
4. Add author names and final notes.
5. Test reproduction from a clean checkout and rehearse the presentation.
