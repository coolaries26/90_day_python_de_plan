CREATE DATABASE ecommerce_db
    WITH OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0;

-- Grant appuser access (reuse existing appuser from dvdrental)
GRANT CONNECT ON DATABASE ecommerce_db TO appuser;
GRANT CREATE ON DATABASE ecommerce_db TO appuser;

\c ecommerce_db

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ml;

-- Grant appuser schema permissions
GRANT USAGE ON SCHEMA raw, analytics, ml TO appuser;
GRANT CREATE ON SCHEMA raw, analytics, ml TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, analytics, ml
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, analytics, ml
    GRANT USAGE, SELECT ON SEQUENCES TO appuser;
