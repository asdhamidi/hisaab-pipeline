DROP TABLE IF EXISTS SILVER.SILVER_ENTRIES;

CREATE TABLE SILVER.SILVER_ENTRIES (
	DATE DATE,
	ITEMS VARCHAR(100),
	PAID_BY VARCHAR(25),
	NOTES VARCHAR(250),
	PRICE NUMERIC(10, 2),
	ENTRY_UPDATED_AT TIMESTAMP,
	OWED_ALL BOOLEAN,
	OWED_BY VARCHAR(100),
	PREVIOUS_VERSIONS VARCHAR(2500),
	ENTRY_CREATED_AT TIMESTAMP,
	ENTRY_CREATED_BY VARCHAR(25),
	CREATED_AT TIMESTAMPTZ,
	CREATED_BY VARCHAR(25)
)
