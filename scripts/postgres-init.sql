-- Create additional databases needed by Superset
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'superset'
)\gexec
