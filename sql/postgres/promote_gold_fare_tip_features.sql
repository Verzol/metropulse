BEGIN;

UPDATE audit.publish_run_history
SET status = 'failed',
    completed_at = CURRENT_TIMESTAMP,
    details = 'Superseded by a subsequent publish attempt before validation completed.'
WHERE target_table = 'ml.gold_fare_tip_features'
  AND status = 'started';

TRUNCATE TABLE ml.gold_fare_tip_features;

INSERT INTO ml.gold_fare_tip_features (
    fare_amount,
    tip_amount,
    tip_percent,
    trip_distance,
    pu_location_id,
    do_location_id,
    passenger_count,
    ratecode_id,
    payment_type,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month,
    gold_processed_timestamp
)
SELECT
    fare_amount,
    tip_amount,
    tip_percent,
    trip_distance,
    pu_location_id,
    do_location_id,
    passenger_count,
    ratecode_id,
    payment_type,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month,
    gold_processed_timestamp
FROM staging.gold_fare_tip_features_staging;

INSERT INTO audit.publish_run_history (
    source_dataset,
    target_table,
    source_row_count,
    target_row_count,
    status,
    details
)
SELECT
    's3a://gold/gold_fare_tip_features/',
    'ml.gold_fare_tip_features',
    COUNT(*),
    COUNT(*),
    'started',
    'Staging promoted transactionally; source-target Spark validation pending.'
FROM ml.gold_fare_tip_features;

COMMIT;

DROP TABLE IF EXISTS staging.gold_fare_tip_features_staging;
ANALYZE ml.gold_fare_tip_features;
