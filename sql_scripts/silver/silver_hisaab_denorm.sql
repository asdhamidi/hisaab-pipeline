DROP TABLE IF EXISTS SILVER.SILVER_HISAAB_DENORM;

CREATE TABLE SILVER.SILVER_HISAAB_DENORM (
    username VARCHAR(25),
    date DATE,
    activity VARCHAR(250),
    activity_created_at VARCHAR(25),
    items VARCHAR(100),
    paid_by VARCHAR(25),
    notes VARCHAR(250),
    price NUMERIC(5, 2),
    entry_updated_at VARCHAR(25),
    owed_all BOOLEAN,
    owed_by VARCHAR(100),
    previous_versions VARCHAR(1000),
    entry_created_by VARCHAR(25), 
    admin BOOLEAN,
    user_created_at VARCHAR(25)
)
