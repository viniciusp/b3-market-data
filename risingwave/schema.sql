-- RisingWave schema: consumes trades.raw and maintains incremental views.
-- Applied automatically at boot by the risingwave-init service (idempotent).
-- To re-apply manually: docker compose exec -T postgres \
--   psql -h risingwave -p 4566 -d dev -U root -f - < risingwave/schema.sql

CREATE SOURCE IF NOT EXISTS trades_raw (
    schema_version INT,
    ticker VARCHAR,
    trade_id BIGINT,
    action INT,
    -- Money arrives as a JSON string; cast to DECIMAL at use sites.
    price VARCHAR,
    quantity BIGINT,
    traded_at TIMESTAMPTZ,
    trade_date DATE,
    reference_date DATE,
    session_type INT,
    source VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'trades.raw',
    properties.bootstrap.server = 'kafka:19092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- First validation view: event counts per (ticker, session), checkable against
-- known totals from the ingestion logs and watermarks.
CREATE MATERIALIZED VIEW IF NOT EXISTS session_event_counts AS
SELECT ticker, trade_date, count(*) AS events
FROM trades_raw
GROUP BY ticker, trade_date;

-- Deduplicated, cancellation-aware trades: at-least-once redeliveries and B3's
-- cross-file relistings collapse into one row per (ticker, session, id); a
-- cancellation row (action=2) anywhere in the group wins ("last action").
CREATE MATERIALIZED VIEW IF NOT EXISTS trades_clean AS
SELECT
    ticker,
    trade_date,
    trade_id,
    max(price::DECIMAL) AS price,    -- duplicate rows carry identical values
    max(quantity)       AS quantity,
    max(session_type)   AS session_type,
    -- Relisted after-market rows carry a bogus early-morning timestamp on the
    -- session's own date; max() keeps the real execution time.
    max(traded_at)      AS traded_at,
    max(action) = 2     AS cancelled
FROM trades_raw
-- Measured source behavior: shape-mismatched JSON becomes NULL-filled rows and
-- type-mismatched fields become NULLs (never a stall). Guard the key columns.
WHERE ticker IS NOT NULL AND trade_date IS NOT NULL AND trade_id IS NOT NULL
GROUP BY ticker, trade_date, trade_id;

-- Latest valid trade per ticker (Top-N pattern).
CREATE MATERIALIZED VIEW IF NOT EXISTS last_price AS
SELECT ticker, price, traded_at
FROM (
    SELECT ticker, price, traded_at,
           row_number() OVER (PARTITION BY ticker ORDER BY traded_at DESC, trade_id DESC) AS rn
    FROM trades_clean
    WHERE NOT cancelled
) ranked
WHERE rn = 1;

-- Daily per-session summary. OHLC follows B3's official convention: regular
-- session only (after-market excluded from prices); quantity, financial volume
-- and trade count include every session type, matching COTAHIST totals.
CREATE MATERIALIZED VIEW IF NOT EXISTS session_summary AS
SELECT
    ticker,
    trade_date,
    first_value(price ORDER BY traded_at, trade_id) FILTER (WHERE session_type = 1) AS open,
    max(price) FILTER (WHERE session_type = 1) AS high,
    min(price) FILTER (WHERE session_type = 1) AS low,
    last_value(price ORDER BY traded_at, trade_id) FILTER (WHERE session_type = 1) AS close,
    sum(quantity)::BIGINT AS quantity,
    sum(price * quantity) AS financial_volume,
    count(*) AS trades
FROM trades_clean
WHERE NOT cancelled
GROUP BY ticker, trade_date;

-- Serving sinks: upsert into Postgres, the only store the API reads.
-- Plaintext credentials are acceptable here: compose-internal dev setup.
CREATE SINK IF NOT EXISTS last_price_sink FROM last_price WITH (
    connector = 'postgres',
    host = 'postgres',
    port = '5432',
    user = 'app',
    password = 'app',
    database = 'b3',
    table = 'last_price',
    type = 'upsert',
    primary_key = 'ticker'
);

CREATE SINK IF NOT EXISTS session_summary_sink FROM session_summary WITH (
    connector = 'postgres',
    host = 'postgres',
    port = '5432',
    user = 'app',
    password = 'app',
    database = 'b3',
    table = 'session_summary',
    type = 'upsert',
    primary_key = 'ticker,trade_date'
);
