-- Creates required databases for Airflow and Hisaab Analytics, if they do not exist.
-- Grants all privileges on the created databases to the airflow user.
-- Check and create airflow database if not exists
SELECT 'CREATE DATABASE airflow WITH OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Grant privileges on airflow database
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Check and create hisaab_analytics database if not exists
SELECT 'CREATE DATABASE hisaab_analytics WITH OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hisaab_analytics')\gexec

-- Grant privileges on hisaab_analytics database
GRANT ALL PRIVILEGES ON DATABASE hisaab_analytics TO airflow;