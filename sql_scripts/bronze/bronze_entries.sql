DROP TABLE IF EXISTS BRONZE.BRONZE_ENTRIES;

CREATE TABLE BRONZE.BRONZE_ENTRIES (
    entry_created_at VARCHAR(25),
    entry_created_by VARCHAR(25),
    date VARCHAR(25),
    items VARCHAR(100),
    notes VARCHAR(250),
    owed_all VARCHAR(25),
    owed_by VARCHAR(100),
    paid_by VARCHAR(25),
    previous_versions VARCHAR(2500),
    price VARCHAR(25),
    updated_at VARCHAR(25),
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
