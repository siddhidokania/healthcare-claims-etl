-- One-time Snowflake account setup. Run in a Snowsight worksheet as ACCOUNTADMIN.
--
-- Do not run this template directly: `python -m warehouse_snowflake.make_setup_sql`
-- writes 00_account_setup.local.sql with your service user's public key filled in.
--
-- Creates:
--   * role CLAIMS_ENGINEER
--   * three warehouses (XSMALL, SMALL, MEDIUM) for the sizing experiment,
--     all auto-suspending after 60 seconds
--   * resource monitor CLAIMS_MONITOR: 20 credits a month, alerts at 50% / 80%,
--     suspends the warehouses at 90% and kills running queries at 100%
--   * database CLAIMS_DB
--   * service user CLAIMS_SVC that signs in with a key pair (no password)

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS CLAIMS_ENGINEER COMMENT = 'Builds and benchmarks the claims gold layer';
-- ACCOUNTADMIN inherits SYSADMIN, so admins see everything this role creates.
GRANT ROLE CLAIMS_ENGINEER TO ROLE SYSADMIN;

-- Warehouses: same settings, three sizes. Credits per hour: XSMALL 1, SMALL 2, MEDIUM 4.
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH_XS
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Default warehouse for loads and reports';
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH_S
    WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Sizing experiment';
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH_M
    WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Sizing experiment';

-- Cost guardrail for everything this project runs.
CREATE OR REPLACE RESOURCE MONITOR CLAIMS_MONITOR
    WITH CREDIT_QUOTA = 20
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY
             ON 80 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLAIMS_WH_XS SET RESOURCE_MONITOR = CLAIMS_MONITOR;
ALTER WAREHOUSE CLAIMS_WH_S  SET RESOURCE_MONITOR = CLAIMS_MONITOR;
ALTER WAREHOUSE CLAIMS_WH_M  SET RESOURCE_MONITOR = CLAIMS_MONITOR;

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE CLAIMS_WH_XS TO ROLE CLAIMS_ENGINEER;
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE CLAIMS_WH_S  TO ROLE CLAIMS_ENGINEER;
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE CLAIMS_WH_M  TO ROLE CLAIMS_ENGINEER;
GRANT MONITOR ON RESOURCE MONITOR CLAIMS_MONITOR TO ROLE CLAIMS_ENGINEER;
-- Lets the role read warehouse metering for the FinOps report.
GRANT MONITOR USAGE ON ACCOUNT TO ROLE CLAIMS_ENGINEER;

CREATE DATABASE IF NOT EXISTS CLAIMS_DB COMMENT = 'Gold layer of healthcare-claims-etl';
GRANT USAGE, CREATE SCHEMA ON DATABASE CLAIMS_DB TO ROLE CLAIMS_ENGINEER;

-- Service user for the pipeline: key-pair authentication only.
CREATE USER IF NOT EXISTS CLAIMS_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = CLAIMS_ENGINEER
    DEFAULT_WAREHOUSE = CLAIMS_WH_XS
    RSA_PUBLIC_KEY = '{{PUBLIC_KEY}}'
    COMMENT = 'healthcare-claims-etl pipeline';
GRANT ROLE CLAIMS_ENGINEER TO USER CLAIMS_SVC;

-- Check
SHOW WAREHOUSES LIKE 'CLAIMS%';
SHOW RESOURCE MONITORS LIKE 'CLAIMS%';
