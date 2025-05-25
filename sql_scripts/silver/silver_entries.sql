DROP TABLE IF EXISTS SILVER.SILVER_ENTRIES;

CREATE TABLE SILVER.SILVER_ENTRIES (
    date DATE,
    items VARCHAR(100),
    paid_by VARCHAR(25),
    notes VARCHAR(250),
    price NUMERIC(5, 2),
    updated_at VARCHAR(25),
    owed_all BOOLEAN,
    owed_by VARCHAR(100),
    previous_versions VARCHAR(1000),
    entry_created_at TIMESTAMP,
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
