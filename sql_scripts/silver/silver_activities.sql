DROP TABLE IF EXISTS SILVER.SILVER_ACTIVITIES;

CREATE TABLE SILVER.SILVER_ACTIVITIES (
    username VARCHAR(25),
    date DATE,
    activity VARCHAR(250),
    activity_created_at TIMESTAMP,
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
