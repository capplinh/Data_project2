-- =============================================
-- Gold Layer - Dimension Customers
-- Mục đích: Dimension table cho khách hàng (SCD Type 2)
-- =============================================

CREATE TABLE dim_customers (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    full_address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    customer_type VARCHAR(50),
    business_type VARCHAR(50),
    credit_limit DECIMAL(18,2),
    registration_date DATE,
    status VARCHAR(20),
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes
    CONSTRAINT uk_customer_id_effective UNIQUE (customer_id, effective_date),
    INDEX idx_dim_customers_id (customer_id),
    INDEX idx_dim_customers_current (is_current),
    INDEX idx_dim_customers_city (city),
    INDEX idx_dim_customers_region (region)
);
