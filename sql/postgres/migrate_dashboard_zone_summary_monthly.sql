BEGIN;

LOCK TABLE mart.dashboard_zone_summary IN ACCESS EXCLUSIVE MODE;
TRUNCATE TABLE mart.dashboard_zone_summary;

ALTER TABLE mart.dashboard_zone_summary
    DROP CONSTRAINT IF EXISTS dashboard_zone_summary_pkey;

ALTER TABLE mart.dashboard_zone_summary
    ADD COLUMN IF NOT EXISTS pickup_year_month VARCHAR(7);

ALTER TABLE mart.dashboard_zone_summary
    ALTER COLUMN pickup_year_month SET NOT NULL;

ALTER TABLE mart.dashboard_zone_summary
    ADD CONSTRAINT dashboard_zone_summary_pkey
    PRIMARY KEY (pickup_year_month, pu_location_id);

CREATE INDEX IF NOT EXISTS dashboard_zone_summary_month_idx
    ON mart.dashboard_zone_summary (pickup_year_month);

CREATE INDEX IF NOT EXISTS dashboard_zone_summary_demand_idx
    ON mart.dashboard_zone_summary (total_demand DESC);

COMMIT;
