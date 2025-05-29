DROP TABLE IF EXISTS SILVER.SILVER_HISAAB_DENORM;

CREATE TABLE SILVER.SILVER_HISAAB_DENORM (
    date DATE,
    username VARCHAR(25),
    admin BOOLEAN,
    items VARCHAR(250),
    price NUMERIC(5, 2),
    paid_by VARCHAR(25),
    owed_all BOOLEAN,
    owed_by VARCHAR(100),
    activity VARCHAR(250),
    notes VARCHAR(250),
    previous_versions VARCHAR(2500),
    entry_updated_at VARCHAR(25),
    entry_created_at VARCHAR(25),
    activity_created_at VARCHAR(25),
    user_created_at VARCHAR(25),
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
