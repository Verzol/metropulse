BEGIN;

UPDATE audit.publish_run_history
SET status = 'failed',
    completed_at = CURRENT_TIMESTAMP,
    details = 'Superseded by a subsequent publish attempt before validation completed.'
WHERE target_table = 'ml.gold_demand_features'
  AND status = 'started';

TRUNCATE TABLE ml.gold_demand_features;

INSERT INTO ml.gold_demand_features (
    pu_location_id,
    pickup_hour,
    demand,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month,
    gold_processed_timestamp
)
SELECT
    pu_location_id,
    pickup_hour,
    demand,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month,
    gold_processed_timestamp
FROM staging.gold_demand_features_staging;

INSERT INTO audit.publish_run_history (
    source_dataset,
    target_table,
    source_row_count,
    target_row_count,
    status,
    details
)
SELECT
    's3a://gold/gold_demand_features/',
    'ml.gold_demand_features',
    COUNT(*),
    COUNT(*),
    'started',
    'Staging promoted transactionally; source-target Spark validation pending.'
FROM ml.gold_demand_features;

COMMIT;

ANALYZE ml.gold_demand_features;
