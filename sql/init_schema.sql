CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.charging_sessions (
    acn_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    user_id TEXT,
    station_id TEXT,
    space_id TEXT,
    site_id TEXT,
    cluster_id TEXT,
    connection_time_raw TEXT,
    disconnect_time_raw TEXT,
    done_charging_time_raw TEXT,
    timezone TEXT,
    kwh_delivered NUMERIC,
    user_inputs_json JSONB,
    json_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_charging_sessions_session_id
    ON raw.charging_sessions (session_id);

CREATE INDEX IF NOT EXISTS idx_raw_charging_sessions_station_id
    ON raw.charging_sessions (station_id);

CREATE INDEX IF NOT EXISTS idx_raw_charging_sessions_site_id
    ON raw.charging_sessions (site_id);