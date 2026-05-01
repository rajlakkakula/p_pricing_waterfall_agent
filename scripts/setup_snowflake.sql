-- ============================================================
-- Snowflake setup for Price-to-Margin Waterfall Agent
-- Compatible with: Standard Edition (personal account)
--
-- Run this once as ACCOUNTADMIN or SYSADMIN before loading data.
-- Execute in Snowflake Web UI (Snowsight) or SnowSQL.
-- ============================================================

-- ── 1. Warehouse (XS, auto-suspend after 60s to save credits) ───────────────
CREATE WAREHOUSE IF NOT EXISTS PRICING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Waterfall agent compute warehouse';

USE WAREHOUSE PRICING_WH;

-- ── 2. Database and medallion schemas ────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS PRICING_DB
    COMMENT = 'Price-to-Margin Waterfall Intelligence';

CREATE SCHEMA IF NOT EXISTS PRICING_DB.BRONZE
    COMMENT = 'Raw ERP load — append-only';

CREATE SCHEMA IF NOT EXISTS PRICING_DB.SILVER
    COMMENT = 'Deduplicated and validated transactions';

CREATE SCHEMA IF NOT EXISTS PRICING_DB.GOLD
    COMMENT = 'Analytics-ready waterfall facts and aggregates';

-- ── 3. Role and user (optional — skip if using your default SYSADMIN role) ──
-- CREATE ROLE IF NOT EXISTS PRICING_READONLY;
-- GRANT USAGE ON WAREHOUSE PRICING_WH         TO ROLE PRICING_READONLY;
-- GRANT USAGE ON DATABASE  PRICING_DB          TO ROLE PRICING_READONLY;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE PRICING_DB TO ROLE PRICING_READONLY;
-- GRANT SELECT ON ALL TABLES  IN DATABASE PRICING_DB TO ROLE PRICING_READONLY;
-- GRANT SELECT ON ALL VIEWS   IN DATABASE PRICING_DB TO ROLE PRICING_READONLY;
-- GRANT ROLE PRICING_READONLY TO USER <your_username>;

-- ── 4. Bronze source table ────────────────────────────────────────────────────
USE SCHEMA PRICING_DB.BRONZE;

CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    -- Dimension columns
    country          VARCHAR(100)    NOT NULL,
    year             INTEGER         NOT NULL,
    material         VARCHAR(50)     NOT NULL,
    sales_designation VARCHAR(100)   NOT NULL,
    sold_to          VARCHAR(50)     NOT NULL,
    corporate_group  VARCHAR(200),

    -- Volume
    sales_qty        DECIMAL(18, 4)  NOT NULL,

    -- Waterfall price columns (CLAUDE.md §Data Schema)
    blue_jobber_price DECIMAL(18, 4) NOT NULL,
    deductions        DECIMAL(18, 4) NOT NULL DEFAULT 0,
    invoice_price     DECIMAL(18, 4) NOT NULL,
    bonuses           DECIMAL(18, 4) NOT NULL DEFAULT 0,
    pocket_price      DECIMAL(18, 4) NOT NULL,

    -- Org dimension
    pso              VARCHAR(100)    NOT NULL,

    -- Cost columns
    standard_cost    DECIMAL(18, 4),
    material_cost    DECIMAL(18, 4),

    -- ETL metadata
    _loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw ERP transactions — append-only, never updated';

-- ── 5. Internal stage for CSV upload ─────────────────────────────────────────
CREATE STAGE IF NOT EXISTS PRICING_DB.BRONZE.RAW_TXN_STAGE
    FILE_FORMAT = (
        TYPE             = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER      = 1
        NULL_IF          = ('', 'NULL', 'null')
        EMPTY_FIELD_AS_NULL = TRUE
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    )
    COMMENT = 'Staging area for CSV uploads';

-- ── 6. Verify setup ───────────────────────────────────────────────────────────
SHOW SCHEMAS IN DATABASE PRICING_DB;
SHOW TABLES  IN SCHEMA   PRICING_DB.BRONZE;
SHOW STAGES  IN SCHEMA   PRICING_DB.BRONZE;

-- Expected output:
--   Schemas : BRONZE, SILVER, GOLD
--   Tables  : RAW_TRANSACTIONS
--   Stages  : RAW_TXN_STAGE
