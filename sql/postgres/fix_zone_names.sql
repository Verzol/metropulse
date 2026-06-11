BEGIN;

CREATE TEMP TABLE tmp_zone_lookup (
    LocationID int,
    Borough text,
    Zone text,
    service_zone text
);

\copy tmp_zone_lookup(LocationID, Borough, Zone, service_zone) FROM '/home/verzol/metropulse/data/taxi_zone_lookup.csv' DELIMITER ',' CSV HEADER;

UPDATE mart.dashboard_zone_summary s
SET pickup_borough = t.Borough,
    pickup_zone = t.Zone
FROM tmp_zone_lookup t
WHERE s.pu_location_id = t.LocationID;

COMMIT;
