IF OBJECT_ID(N'stream.events_raw', N'U') IS NULL
CREATE TABLE stream.events_raw (
    raw_id         BIGINT         IDENTITY(1,1) NOT NULL,
    event_id       VARCHAR(64)    NOT NULL,
    event_type     NVARCHAR(40)   NOT NULL,
    order_id       INT            NOT NULL,
    event_ts       DATETIME2(0)   NOT NULL,
    payload_json   NVARCHAR(MAX)  NOT NULL,
    partition_id   INT            NOT NULL,
    ingested_at    DATETIME2(0)   NOT NULL CONSTRAINT DF_stream_raw_ing DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_stream_events_raw PRIMARY KEY (raw_id)
);
GO

IF OBJECT_ID(N'stream.events', N'U') IS NULL
CREATE TABLE stream.events (
    event_id       VARCHAR(64)    NOT NULL,
    event_type     NVARCHAR(40)   NOT NULL,
    order_id       INT            NOT NULL,
    customer_id    INT            NULL,
    city           NVARCHAR(60)   NULL,
    product_id     INT            NULL,
    quantity       INT            NULL,
    order_amount   DECIMAL(12,2)  NULL,
    payment_method NVARCHAR(20)   NULL,
    event_ts       DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_stream_events PRIMARY KEY (event_id)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_stream_events_ts')
    CREATE INDEX IX_stream_events_ts ON stream.events (event_ts);
GO

IF OBJECT_ID(N'stream.revenue_windows', N'U') IS NULL
CREATE TABLE stream.revenue_windows (
    window_start   DATETIME2(0)   NOT NULL,
    window_end     DATETIME2(0)   NOT NULL,
    paid_events    INT            NOT NULL,
    revenue        DECIMAL(12,2)  NOT NULL,
    closed_at      DATETIME2(0)   NOT NULL,
    CONSTRAINT PK_stream_windows PRIMARY KEY (window_start)
);
GO
