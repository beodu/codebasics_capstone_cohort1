-- AtliQ Commerce warehouse. Medallion schemas on SQL Server.
-- Bronze = raw landing (plus dated incremental batches).
-- Silver = cleaned, de-duplicated, merged current state.
-- Gold   = star schema for reporting.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'bronze') EXEC(N'CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'silver') EXEC(N'CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')   EXEC(N'CREATE SCHEMA gold');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ops')    EXEC(N'CREATE SCHEMA ops');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stream') EXEC(N'CREATE SCHEMA stream');
GO

IF OBJECT_ID(N'ops.pipeline_run_log', N'U') IS NULL
CREATE TABLE ops.pipeline_run_log (
    log_id         INT            IDENTITY(1,1) NOT NULL,
    run_id         VARCHAR(16)    NOT NULL,
    run_date       DATE           NOT NULL,
    stage          NVARCHAR(40)   NOT NULL,
    table_name     NVARCHAR(128)  NOT NULL,
    rows_in        INT            NOT NULL,
    rows_after     INT            NOT NULL,
    status         NVARCHAR(20)   NOT NULL,
    error_message  NVARCHAR(4000) NULL,
    started_at     DATETIME2(0)   NOT NULL,
    finished_at    DATETIME2(0)   NOT NULL,
    duration_secs  DECIMAL(10,3)  NOT NULL,
    CONSTRAINT PK_pipeline_run_log PRIMARY KEY (log_id)
);
GO

IF OBJECT_ID(N'bronze.customers', N'U') IS NULL
CREATE TABLE bronze.customers (
    customer_id    INT            NOT NULL,
    customer_name  NVARCHAR(100)  NULL,
    email          NVARCHAR(150)  NULL,
    city           NVARCHAR(60)   NULL,
    signup_date    DATE           NULL,
    updated_at     DATETIME2(0)   NULL,
    ingest_date    DATE           NOT NULL
);
GO

IF OBJECT_ID(N'bronze.products', N'U') IS NULL
CREATE TABLE bronze.products (
    product_id    INT            NOT NULL,
    product_name  NVARCHAR(120)  NULL,
    category      NVARCHAR(60)   NULL,
    unit_price    DECIMAL(10,2)  NULL,
    updated_at    DATETIME2(0)   NULL,
    ingest_date   DATE           NOT NULL
);
GO

IF OBJECT_ID(N'bronze.orders', N'U') IS NULL
CREATE TABLE bronze.orders (
    order_id      INT            NOT NULL,
    customer_id   INT            NULL,
    order_date    DATE           NULL,
    status        NVARCHAR(20)   NULL,
    order_amount  DECIMAL(12,2)  NULL,
    created_at    DATETIME2(0)   NULL,
    updated_at    DATETIME2(0)   NULL,
    ingest_date   DATE           NOT NULL
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_bronze_orders_ingest')
    CREATE INDEX IX_bronze_orders_ingest ON bronze.orders (ingest_date);
GO

IF OBJECT_ID(N'bronze.order_items', N'U') IS NULL
CREATE TABLE bronze.order_items (
    order_item_id  INT            NOT NULL,
    order_id       INT            NULL,
    product_id     INT            NULL,
    quantity       INT            NULL,
    item_price     DECIMAL(10,2)  NULL,
    created_at     DATETIME2(0)   NULL,
    ingest_date    DATE           NOT NULL
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_bronze_items_ingest')
    CREATE INDEX IX_bronze_items_ingest ON bronze.order_items (ingest_date);
GO

IF OBJECT_ID(N'bronze.payments', N'U') IS NULL
CREATE TABLE bronze.payments (
    payment_id  INT            NOT NULL,
    order_id    INT            NULL,
    amount      DECIMAL(12,2)  NULL,
    method      NVARCHAR(20)   NULL,
    paid_at     DATETIME2(0)   NULL,
    updated_at  DATETIME2(0)   NULL,
    ingest_date DATE           NOT NULL
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_bronze_payments_ingest')
    CREATE INDEX IX_bronze_payments_ingest ON bronze.payments (ingest_date);
GO

IF OBJECT_ID(N'bronze.supplier_price_list', N'U') IS NULL
CREATE TABLE bronze.supplier_price_list (
    product_id      INT            NOT NULL,
    product_name    NVARCHAR(120)  NULL,
    supplier_name   NVARCHAR(120)  NULL,
    supplier_cost   DECIMAL(10,2)  NULL,
    effective_date  DATE           NULL,
    ingest_date     DATE           NOT NULL
);
GO

IF OBJECT_ID(N'bronze.marketing_spend', N'U') IS NULL
CREATE TABLE bronze.marketing_spend (
    spend_date    DATE           NOT NULL,
    channel       NVARCHAR(60)   NULL,
    campaign      NVARCHAR(80)   NULL,
    spend_amount  DECIMAL(12,2)  NULL,
    clicks        INT            NULL,
    ingest_date   DATE           NOT NULL
);
GO

IF OBJECT_ID(N'silver.customers', N'U') IS NULL
CREATE TABLE silver.customers (
    customer_id    INT            NOT NULL,
    customer_name  NVARCHAR(100)  NOT NULL,
    email          NVARCHAR(150)  NOT NULL,
    city           NVARCHAR(60)   NOT NULL,
    signup_date    DATE           NOT NULL,
    updated_at     DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_silver_customers PRIMARY KEY (customer_id)
);
GO

IF OBJECT_ID(N'silver.products', N'U') IS NULL
CREATE TABLE silver.products (
    product_id    INT            NOT NULL,
    product_name  NVARCHAR(120)  NOT NULL,
    category      NVARCHAR(60)   NOT NULL,
    unit_price    DECIMAL(10,2)  NOT NULL,
    updated_at    DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_silver_products PRIMARY KEY (product_id)
);
GO

IF OBJECT_ID(N'silver.orders', N'U') IS NULL
CREATE TABLE silver.orders (
    order_id      INT            NOT NULL,
    customer_id   INT            NOT NULL,
    order_date    DATE           NOT NULL,
    status        NVARCHAR(20)   NOT NULL,
    order_amount  DECIMAL(12,2)  NOT NULL,
    created_at    DATETIME2(0)   NOT NULL,
    updated_at    DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_silver_orders PRIMARY KEY (order_id)
);
GO

IF OBJECT_ID(N'silver.order_items', N'U') IS NULL
CREATE TABLE silver.order_items (
    order_item_id  INT            NOT NULL,
    order_id       INT            NOT NULL,
    product_id     INT            NOT NULL,
    quantity       INT            NOT NULL,
    item_price     DECIMAL(10,2)  NOT NULL,
    created_at     DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_silver_order_items PRIMARY KEY (order_item_id)
);
GO

IF OBJECT_ID(N'silver.payments', N'U') IS NULL
CREATE TABLE silver.payments (
    payment_id  INT            NOT NULL,
    order_id    INT            NOT NULL,
    amount      DECIMAL(12,2)  NOT NULL,
    method      NVARCHAR(20)   NOT NULL,
    paid_at     DATETIME2(0)   NOT NULL,
    updated_at  DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_silver_payments PRIMARY KEY (payment_id)
);
GO

IF OBJECT_ID(N'silver.supplier_price_list', N'U') IS NULL
CREATE TABLE silver.supplier_price_list (
    product_id      INT            NOT NULL,
    supplier_name   NVARCHAR(120)  NOT NULL,
    supplier_cost   DECIMAL(10,2)  NOT NULL,
    effective_date  DATE           NOT NULL,
    CONSTRAINT PK_silver_supplier PRIMARY KEY (product_id)
);
GO

IF OBJECT_ID(N'silver.marketing_spend', N'U') IS NULL
CREATE TABLE silver.marketing_spend (
    spend_date    DATE           NOT NULL,
    channel       NVARCHAR(60)   NOT NULL,
    campaign      NVARCHAR(80)   NOT NULL,
    spend_amount  DECIMAL(12,2)  NOT NULL,
    clicks        INT            NOT NULL,
    CONSTRAINT PK_silver_mkt PRIMARY KEY (spend_date, channel, campaign)
);
GO
