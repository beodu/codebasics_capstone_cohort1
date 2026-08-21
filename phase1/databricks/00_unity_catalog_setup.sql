-- ============================================================================
--  AtliQ Commerce | Unity Catalog one-time setup
--  Run once, as workspace admin, before the first Silver build.
--
--  Prerequisites done in the Azure portal (not SQL):
--    1. Create an Azure Databricks Access Connector (a managed identity).
--    2. Grant it "Storage Blob Data Contributor" on the ADLS Gen2 account.
--  The access connector is the modern replacement for /mnt/... mounts, which
--  are legacy and being retired. No storage keys are ever put in a notebook.
-- ============================================================================

-- 1. Storage credential wrapping the access connector
CREATE STORAGE CREDENTIAL IF NOT EXISTS atliq_adls_cred
    WITH AZURE_MANAGED_IDENTITY (
        ACCESS_CONNECTOR_ID = '/subscriptions/<sub-id>/resourceGroups/rg-atliq-capstone/providers/Microsoft.Databricks/accessConnectors/atliq-access-connector'
    )
    COMMENT 'Managed identity for the AtliQ lakehouse storage account';

-- 2. External location over the lakehouse container
CREATE EXTERNAL LOCATION IF NOT EXISTS atliq_lakehouse
    URL 'abfss://lakehouse@<storage_account>.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL atliq_adls_cred)
    COMMENT 'Root of the AtliQ medallion lakehouse';

-- 3. Catalog and schemas
CREATE CATALOG IF NOT EXISTS atliq;
CREATE SCHEMA  IF NOT EXISTS atliq.silver COMMENT 'Cleaned, de-duplicated, governed source of truth';
CREATE SCHEMA  IF NOT EXISTS atliq.gold   COMMENT 'Star schema built by dbt, served to Fabric';
CREATE SCHEMA  IF NOT EXISTS atliq.ops    COMMENT 'Pipeline run audit log';

-- 4. Run audit table (written by every Silver notebook run)
CREATE TABLE IF NOT EXISTS atliq.ops.pipeline_run_log (
    run_id          STRING,
    run_date        DATE,
    stage           STRING,     -- bronze_to_silver | gold_build
    table_name      STRING,
    rows_in         BIGINT,     -- rows read from the batch
    rows_after      BIGINT,     -- rows in the target after the merge
    status          STRING,     -- SUCCESS | FAILED
    error_message   STRING,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    duration_secs   DOUBLE
) USING DELTA
COMMENT 'One row per table per stage per run. Every nightly run leaves a trace here.';

-- 5. Least-privilege grants for the reporting/CI principal
-- GRANT USAGE ON CATALOG atliq TO `atliq-readers`;
-- GRANT SELECT ON SCHEMA atliq.gold TO `atliq-readers`;
