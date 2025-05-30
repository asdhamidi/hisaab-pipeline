DROP TABLE IF EXISTS SILVER.SILVER_TIME_DIM;

CREATE TABLE SILVER.SILVER_TIME_DIM (
    date DATE NOT NULL,
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
    fiscal_quarter INTEGER
);

-- Create index on date column for better join performance
CREATE INDEX idx_time_dimension_date ON SILVER.SILVER_TIME_DIM (date);
