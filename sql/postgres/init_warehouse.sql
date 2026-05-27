-- MetroPulse PostgreSQL serving warehouse foundation.
-- Gold Parquet on MinIO remains the source of truth; this database serves consumers.

SET TIME ZONE 'America/New_York';
SELECT format(
    'ALTER DATABASE %I SET timezone TO %L',
    current_database(),
    'America/New_York'
) \gexec

CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS staging;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ml_reader') THEN
        CREATE ROLE ml_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
        CREATE ROLE dashboard_reader NOLOGIN;
    END IF;
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA ml TO ml_reader;
GRANT USAGE ON SCHEMA mart TO dashboard_reader;
REVOKE ALL ON SCHEMA staging FROM PUBLIC;
REVOKE ALL ON SCHEMA staging FROM ml_reader;
REVOKE ALL ON SCHEMA staging FROM dashboard_reader;

CREATE TABLE IF NOT EXISTS ml.gold_demand_features (
    pu_location_id SMALLINT NOT NULL,
    pickup_hour TIMESTAMP NOT NULL,
    demand INTEGER NOT NULL CHECK (demand > 0),
    hour SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    temperature_f REAL NOT NULL,
    precipitation_mm REAL NOT NULL,
    pickup_year_month VARCHAR(7) NOT NULL,
    gold_processed_timestamp TIMESTAMP NOT NULL,
    CONSTRAINT gold_demand_features_pk PRIMARY KEY (pu_location_id, pickup_hour)
);

CREATE INDEX IF NOT EXISTS gold_demand_features_pickup_hour_idx
    ON ml.gold_demand_features (pickup_hour);
CREATE INDEX IF NOT EXISTS gold_demand_features_year_month_idx
    ON ml.gold_demand_features (pickup_year_month);

CREATE TABLE IF NOT EXISTS mart.dashboard_hourly_demand_kpi (
    pickup_hour TIMESTAMP NOT NULL PRIMARY KEY,
    hour SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    pickup_year_month VARCHAR(7) NOT NULL,
    total_demand BIGINT NOT NULL CHECK (total_demand > 0),
    active_zones INTEGER NOT NULL CHECK (active_zones > 0),
    avg_demand_per_active_zone DOUBLE PRECISION NOT NULL,
    max_zone_hour_demand INTEGER NOT NULL CHECK (max_zone_hour_demand > 0),
    avg_temperature_f DOUBLE PRECISION NOT NULL,
    avg_precipitation_mm DOUBLE PRECISION NOT NULL,
    source_gold_processed_timestamp TIMESTAMP NOT NULL,
    dashboard_processed_timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS dashboard_hourly_demand_month_idx
    ON mart.dashboard_hourly_demand_kpi (pickup_year_month);

CREATE TABLE IF NOT EXISTS mart.dashboard_zone_summary (
    pu_location_id SMALLINT NOT NULL PRIMARY KEY,
    pickup_borough TEXT,
    pickup_zone TEXT,
    pickup_latitude DOUBLE PRECISION,
    pickup_longitude DOUBLE PRECISION,
    total_demand BIGINT NOT NULL CHECK (total_demand > 0),
    avg_hourly_demand DOUBLE PRECISION NOT NULL,
    max_hourly_demand INTEGER NOT NULL CHECK (max_hourly_demand > 0),
    active_hours INTEGER NOT NULL CHECK (active_hours > 0),
    first_pickup_hour TIMESTAMP NOT NULL,
    last_pickup_hour TIMESTAMP NOT NULL,
    avg_temperature_f DOUBLE PRECISION NOT NULL,
    avg_precipitation_mm DOUBLE PRECISION NOT NULL,
    source_gold_processed_timestamp TIMESTAMP NOT NULL,
    dashboard_processed_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dashboard_payment_tip_summary (
    pickup_year_month VARCHAR(7) NOT NULL,
    payment_type SMALLINT,
    trip_count BIGINT NOT NULL CHECK (trip_count > 0),
    avg_fare_amount DOUBLE PRECISION NOT NULL,
    avg_tip_amount DOUBLE PRECISION NOT NULL,
    avg_tip_percent DOUBLE PRECISION NOT NULL,
    median_tip_percent DOUBLE PRECISION NOT NULL,
    median_fare_amount DOUBLE PRECISION NOT NULL,
    avg_trip_distance DOUBLE PRECISION NOT NULL,
    min_fare_amount NUMERIC(12, 2) NOT NULL,
    max_fare_amount NUMERIC(12, 2) NOT NULL,
    min_tip_percent DOUBLE PRECISION NOT NULL,
    max_tip_percent DOUBLE PRECISION NOT NULL,
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    source_gold_processed_timestamp TIMESTAMP NOT NULL,
    dashboard_processed_timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS dashboard_payment_tip_month_idx
    ON mart.dashboard_payment_tip_summary (pickup_year_month);

CREATE TABLE IF NOT EXISTS audit.publish_run_history (
    publish_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_dataset TEXT NOT NULL,
    target_table TEXT NOT NULL,
    source_row_count BIGINT,
    target_row_count BIGINT,
    status TEXT NOT NULL CHECK (status IN ('started', 'passed', 'failed')),
    details TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.validation_results (
    validation_result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    publish_run_id BIGINT REFERENCES audit.publish_run_history (publish_run_id),
    check_name TEXT NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail')),
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

GRANT SELECT ON ml.gold_demand_features TO ml_reader;
GRANT SELECT ON mart.dashboard_hourly_demand_kpi TO dashboard_reader;
GRANT SELECT ON mart.dashboard_zone_summary TO dashboard_reader;
GRANT SELECT ON mart.dashboard_payment_tip_summary TO dashboard_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA ml REVOKE SELECT ON TABLES FROM ml_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart REVOKE SELECT ON TABLES FROM dashboard_reader;

-- Remove the legacy staging relation that was accidentally visible to ML readers.
DROP TABLE IF EXISTS ml.gold_demand_features_staging;
