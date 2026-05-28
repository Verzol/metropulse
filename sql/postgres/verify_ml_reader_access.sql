-- Validate the ML consumer role without performing writes.

\set ON_ERROR_STOP on

SELECT
    rolname,
    rolcanlogin
FROM pg_roles
WHERE rolname = :'ml_reader_user';

SELECT
    pg_has_role(:'ml_reader_user', 'ml_reader', 'member') AS inherits_ml_reader,
    has_schema_privilege(:'ml_reader_user', 'ml', 'USAGE') AS can_use_ml_schema,
    has_schema_privilege(:'ml_reader_user', 'ml', 'CREATE') AS can_create_in_ml_schema,
    has_schema_privilege(:'ml_reader_user', 'staging', 'USAGE') AS can_use_staging_schema,
    has_table_privilege(:'ml_reader_user', 'ml.gold_demand_features', 'SELECT') AS can_select_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_fare_tip_features', 'SELECT') AS can_select_fare_tip_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_demand_features', 'INSERT') AS can_insert_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_fare_tip_features', 'INSERT') AS can_insert_fare_tip_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_demand_features', 'UPDATE') AS can_update_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_fare_tip_features', 'UPDATE') AS can_update_fare_tip_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_demand_features', 'DELETE') AS can_delete_features,
    has_table_privilege(:'ml_reader_user', 'ml.gold_fare_tip_features', 'DELETE') AS can_delete_fare_tip_features;
