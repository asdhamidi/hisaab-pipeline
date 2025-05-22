DROP TABLE IF EXISTS BRONZE.BRONZE_USERS;

CREATE TABLE BRONZE.BRONZE_USERS (
    admin VARCHAR(5),
    username VARCHAR(25),
    user_created_at VARCHAR(25),
    created_at TIMESTAMP,
    created_by VARCHAR(25)
)
