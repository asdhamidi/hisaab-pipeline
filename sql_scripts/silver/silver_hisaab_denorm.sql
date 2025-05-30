DROP TABLE IF EXISTS SILVER.SILVER_HISAAB_DENORM;

CREATE TABLE SILVER.SILVER_HISAAB_DENORM (
    date DATE,
    username VARCHAR(25),
    admin BOOLEAN,
    items VARCHAR(250),
    price NUMERIC(10, 2),
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
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(9),
    day_short_name VARCHAR(3),
    is_weekend BOOLEAN,
    week_of_month INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(9),
    month_short_name VARCHAR(3),
    quarter INTEGER,
    year INTEGER,
    year_month VARCHAR(7),
    year_quarter VARCHAR(7),
    month_third VARCHAR(6),
    is_leap_year BOOLEAN,
    day_of_year INTEGER,
    first_day_of_month DATE,
    last_day_of_month DATE,
    days_in_month INTEGER,
    is_first_day_of_month BOOLEAN,
    is_last_day_of_month BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    created_at TIMESTAMPTZ,
    created_by VARCHAR(25)
)
