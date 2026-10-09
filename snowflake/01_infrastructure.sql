-- Infrastructure Snowflake — projet NYC Yellow Taxi
--
-- Ce script est rejouable : les créations utilisent IF NOT EXISTS.
-- À exécuter avec un compte humain d'administration, via Run All dans Snowsight.
-- Les programmes (Python et Airflow) n'utilisent jamais ACCOUNTADMIN.
--
-- Noms retenus :
--   warehouse          NYC_TAXI_WH
--   base               NYC_TAXI
--   rôle des outils    NYC_TAXI_ENGINEER
--   utilisateur service NYC_TAXI_AIRFLOW_SVC

-- 1. Rôle des outils et rattachement à la hiérarchie.
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS NYC_TAXI_ENGINEER
  COMMENT = 'Rôle de moindre privilège pour ingestion Python et Airflow NYC Taxi';

GRANT ROLE NYC_TAXI_ENGINEER TO ROLE SYSADMIN;

-- 2. Ressources Snowflake.
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Calcul unique du projet NYC Yellow Taxi';

CREATE DATABASE IF NOT EXISTS NYC_TAXI
  COMMENT = 'Entrepôt du projet NYC Yellow Taxi';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW
  COMMENT = 'Copie fidèle et traçable des fichiers TLC';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.STAGING
  COMMENT = 'Vues de renommage et de typage';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.INTERMEDIATE
  COMMENT = 'Trajets marqués, dédupliqués et enrichis';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.MARTS
  COMMENT = 'Faits, dimensions et tables de consommation';

-- 3. Privilèges minimaux du rôle des outils.
-- Le rôle créera les objets du pipeline et en sera propriétaire.
GRANT USAGE, OPERATE ON WAREHOUSE NYC_TAXI_WH TO ROLE NYC_TAXI_ENGINEER;
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE NYC_TAXI_ENGINEER;

GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA NYC_TAXI.RAW TO ROLE NYC_TAXI_ENGINEER;

GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA NYC_TAXI.STAGING TO ROLE NYC_TAXI_ENGINEER;

GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA NYC_TAXI.INTERMEDIATE TO ROLE NYC_TAXI_ENGINEER;

GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA NYC_TAXI.MARTS TO ROLE NYC_TAXI_ENGINEER;

-- 4. Utilisateur de service.
-- Dans ce compte d'essai, ACCOUNTADMIN est utilisé uniquement pour ce bootstrap
-- d'administration du compte. L'utilisateur créé ne reçoit que NYC_TAXI_ENGINEER,
-- jamais ACCOUNTADMIN.
-- Le compte peut être créé avant la clé. Il ne possède ni mot de passe ni accès Snowsight.
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS NYC_TAXI_AIRFLOW_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = NYC_TAXI_ENGINEER
  DEFAULT_WAREHOUSE = NYC_TAXI_WH
  DEFAULT_NAMESPACE = NYC_TAXI.RAW
  COMMENT = 'Compte de service de l ingestion Python et d Airflow NYC Taxi';

GRANT ROLE NYC_TAXI_ENGINEER TO USER NYC_TAXI_AIRFLOW_SVC;

-- 5. Vérifications à exécuter après le script.
USE ROLE SYSADMIN;
SHOW WAREHOUSES LIKE 'NYC_TAXI_WH';
SHOW SCHEMAS IN DATABASE NYC_TAXI;
SHOW GRANTS TO ROLE NYC_TAXI_ENGINEER;
SHOW GRANTS TO USER NYC_TAXI_AIRFLOW_SVC;

-- La clé publique se configure ensuite avec le fichier local et non versionné :
-- snowflake/02_set_service_public_key.sql.example
