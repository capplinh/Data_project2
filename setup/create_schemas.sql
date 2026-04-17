-- =============================================
-- Create Database Schemas
-- Mục đích: Tạo schemas cho 3 layers
-- =============================================

-- Create Bronze schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create Silver schema
CREATE SCHEMA IF NOT EXISTS silver;

-- Create Gold schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Create ETL Log table
CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    procedure_name VARCHAR(100),
    layer VARCHAR(20),
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_processed INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions (adjust as needed)
-- GRANT USAGE ON SCHEMA bronze TO etl_user;
-- GRANT USAGE ON SCHEMA silver TO etl_user;
-- GRANT USAGE ON SCHEMA gold TO etl_user;
