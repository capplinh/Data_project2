-- =============================================
-- Gold Layer - Dimension Date
-- Mục đích: Dimension table cho thời gian
-- =============================================

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20),
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    
    INDEX idx_dim_date_year_month (year, month),
    INDEX idx_dim_date_quarter (year, quarter)
);
