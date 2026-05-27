BEGIN;

UPDATE audit.publish_run_history
SET status = 'failed',
    completed_at = CURRENT_TIMESTAMP,
    details = 'Superseded by a subsequent publish attempt before validation completed.'
WHERE target_table IN (
        'mart.dashboard_hourly_demand_kpi',
        'mart.dashboard_zone_summary',
        'mart.dashboard_payment_tip_summary'
    )
  AND status = 'started';

TRUNCATE TABLE mart.dashboard_hourly_demand_kpi;
TRUNCATE TABLE mart.dashboard_zone_summary;
TRUNCATE TABLE mart.dashboard_payment_tip_summary;

INSERT INTO mart.dashboard_hourly_demand_kpi
SELECT
    pickup_hour, hour, day_of_week, month, pickup_year_month, total_demand,
    active_zones, avg_demand_per_active_zone, max_zone_hour_demand,
    avg_temperature_f, avg_precipitation_mm, source_gold_processed_timestamp,
    dashboard_processed_timestamp
FROM staging.dashboard_hourly_demand_kpi_staging;

INSERT INTO mart.dashboard_zone_summary
SELECT
    pu_location_id, pickup_borough, pickup_zone, pickup_latitude, pickup_longitude,
    total_demand, avg_hourly_demand, max_hourly_demand, active_hours,
    first_pickup_hour, last_pickup_hour, avg_temperature_f, avg_precipitation_mm,
    source_gold_processed_timestamp, dashboard_processed_timestamp
FROM staging.dashboard_zone_summary_staging;

INSERT INTO mart.dashboard_payment_tip_summary
SELECT
    pickup_year_month, payment_type, trip_count, avg_fare_amount, avg_tip_amount,
    avg_tip_percent, median_tip_percent, median_fare_amount, avg_trip_distance,
    min_fare_amount, max_fare_amount, min_tip_percent, max_tip_percent, month,
    source_gold_processed_timestamp, dashboard_processed_timestamp
FROM staging.dashboard_payment_tip_summary_staging;

INSERT INTO audit.publish_run_history (
    source_dataset, target_table, source_row_count, target_row_count, status, details
)
SELECT 's3a://gold/dashboard_hourly_demand_kpi/', 'mart.dashboard_hourly_demand_kpi',
       COUNT(*), COUNT(*), 'started',
       'Staging promoted transactionally; source-target Spark validation pending.'
FROM mart.dashboard_hourly_demand_kpi
UNION ALL
SELECT 's3a://gold/dashboard_zone_summary/', 'mart.dashboard_zone_summary',
       COUNT(*), COUNT(*), 'started',
       'Staging promoted transactionally; source-target Spark validation pending.'
FROM mart.dashboard_zone_summary
UNION ALL
SELECT 's3a://gold/dashboard_payment_tip_summary/', 'mart.dashboard_payment_tip_summary',
       COUNT(*), COUNT(*), 'started',
       'Staging promoted transactionally; source-target Spark validation pending.'
FROM mart.dashboard_payment_tip_summary;

COMMIT;

ANALYZE mart.dashboard_hourly_demand_kpi;
ANALYZE mart.dashboard_zone_summary;
ANALYZE mart.dashboard_payment_tip_summary;
