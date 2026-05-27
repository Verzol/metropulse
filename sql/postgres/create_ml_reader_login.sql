-- Create or rotate the login used by ML consumers.
-- The password is injected at runtime through psql variables from .env.

\set ON_ERROR_STOP on

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'ml_reader_user', :'ml_reader_password')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'ml_reader_user'
) \gexec

SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'ml_reader_user', :'ml_reader_password') \gexec
SELECT format('ALTER ROLE %I SET timezone TO %L', :'ml_reader_user', 'America/New_York') \gexec
SELECT format('GRANT ml_reader TO %I', :'ml_reader_user') \gexec
