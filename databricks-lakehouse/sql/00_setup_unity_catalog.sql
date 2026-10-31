-- Run once in a Databricks SQL editor / notebook by a metastore admin or catalog creator.
-- Requires a Unity Catalog-enabled workspace (Premium tier on Azure).
-- If your metastore has no root storage, add:  MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/lakehouse'

CREATE CATALOG IF NOT EXISTS lakehouse_dev
  COMMENT 'Enterprise lakehouse - development';

CREATE SCHEMA IF NOT EXISTS lakehouse_dev.bronze COMMENT 'Raw ingested data (strings, append-only)';
CREATE SCHEMA IF NOT EXISTS lakehouse_dev.silver COMMENT 'Cleaned, typed, de-duplicated, SCD2 dimensions';
CREATE SCHEMA IF NOT EXISTS lakehouse_dev.gold   COMMENT 'Business-ready marts for BI';

-- Landing zone for raw files + Auto Loader checkpoints
CREATE VOLUME IF NOT EXISTS lakehouse_dev.bronze.landing
  COMMENT 'Raw CSV drops: customers/ and orders/';

-- Governance: engineers write bronze/silver/gold, analysts only read gold.
-- (Create these groups in your Entra ID / account console first.)
GRANT USE CATALOG ON CATALOG lakehouse_dev TO `data-engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA lakehouse_dev.bronze TO `data-engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA lakehouse_dev.silver TO `data-engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA lakehouse_dev.gold   TO `data-engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME lakehouse_dev.bronze.landing TO `data-engineers`;

GRANT USE CATALOG ON CATALOG lakehouse_dev TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA lakehouse_dev.gold TO `data-analysts`;
