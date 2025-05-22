DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
        RAISE NOTICE 'User airflow created';
    ELSE
        RAISE NOTICE 'User airflow already exists';
    END IF;
END
$$;
  