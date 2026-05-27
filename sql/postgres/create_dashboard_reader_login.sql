-- Create or rotate the login used by dashboard consumers.

\set ON_ERROR_STOP on

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'dashboard_reader_user', :'dashboard_reader_password')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'dashboard_reader_user'
) \gexec

SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'dashboard_reader_user', :'dashboard_reader_password') \gexec
SELECT format('ALTER ROLE %I SET timezone TO %L', :'dashboard_reader_user', 'America/New_York') \gexec
SELECT format('GRANT dashboard_reader TO %I', :'dashboard_reader_user') \gexec
