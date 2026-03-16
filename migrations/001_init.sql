CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS funds (
    scheme_code     TEXT PRIMARY KEY,
    fund_name       TEXT NOT NULL,
    amc             TEXT NOT NULL,
    category        TEXT NOT NULL,
    inception_date  DATE,
    current_nav     NUMERIC(12, 4),
    nav_date        DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nav_data (
    scheme_code TEXT           NOT NULL,
    nav_date    DATE           NOT NULL,
    nav_value   NUMERIC(12, 4) NOT NULL,
    PRIMARY KEY (scheme_code, nav_date)
);

SELECT create_hypertable('nav_data', 'nav_date', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_nav_scheme_date
    ON nav_data (scheme_code, nav_date DESC);

CREATE TABLE IF NOT EXISTS sync_state (
    scheme_code      TEXT PRIMARY KEY,
    backfill_status  TEXT        NOT NULL DEFAULT 'pending',
    last_nav_date    DATE,
    last_synced_at   TIMESTAMPTZ,
    last_sync_run_id TEXT,
    error_count      INT         NOT NULL DEFAULT 0,
    last_error       TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT chk_backfill_status
        CHECK (backfill_status IN ('pending','in_progress','complete','error'))
);

CREATE TABLE IF NOT EXISTS analytics_results (
    scheme_code  TEXT        NOT NULL,
    win_window   TEXT        NOT NULL,
    result_json  JSONB       NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (scheme_code, win_window),
    CONSTRAINT chk_window CHECK (win_window IN ('1Y','3Y','5Y','10Y'))
);

CREATE INDEX IF NOT EXISTS idx_analytics_scheme
    ON analytics_results (scheme_code);

CREATE TABLE IF NOT EXISTS tracked_schemes (
    scheme_code TEXT PRIMARY KEY,
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    added_at    TIMESTAMPTZ DEFAULT NOW()
);