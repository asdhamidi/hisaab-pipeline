DROP TABLE IF EXISTS BRONZE.BRONZE_USERS;

CREATE TABLE BRONZE.BRONZE_USERS (
    admin VARCHAR(5),
    user_created_at VARCHAR(25),
    username VARCHAR(25),
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
