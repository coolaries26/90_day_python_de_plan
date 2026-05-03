-- Create dedicated Airflow user
CREATE USER airflow WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    PASSWORD 'Airflow@2024!';

-- Create Airflow metadata database
CREATE DATABASE airflow_meta
    WITH OWNER = airflow
    ENCODING = 'UTF8'
    TEMPLATE = template0;

-- Grant full access to airflow user on its own DB
GRANT ALL PRIVILEGES ON DATABASE airflow_meta TO airflow;

-- Connect to airflow_meta and grant schema permissions
\c airflow_meta
GRANT ALL ON SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON SEQUENCES TO airflow;

-- Verify the user and its privileges
\du airflow
-- Verify the database creation and ownership
\l airflow_meta
\dn public
\dt public.*
