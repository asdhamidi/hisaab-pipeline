DROP TABLE IF EXISTS SILVER.SILVER_ENTRIES;

CREATE TABLE SILVER.SILVER_ENTRIES (
    date DATE,
    items VARCHAR(100),
    paid_by VARCHAR(25),
    notes VARCHAR(250),
    price NUMERIC(5, 2),
    updated_at TIMESTAMP,
    owed_all BOOLEAN,
    owed_by VARCHAR(100),
    previous_versions VARCHAR(2500),
    entry_created_at TIMESTAMP,
    entry_created_by VARCHAR(25),
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
