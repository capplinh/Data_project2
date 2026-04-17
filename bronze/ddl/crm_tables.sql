-- =============================================
-- Bronze Layer - CRM Tables DDL
-- Mục đích: Lưu trữ dữ liệu thô từ CRM System
-- =============================================

-- Table: crm_cust_info
CREATE TABLE crm_cust_info (
    cust_id VARCHAR(50),
    cust_name VARCHAR(200),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    customer_type VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CRM'
);

-- Table: crm_prd_info
CREATE TABLE crm_prd_info (
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    description TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(18,2),
    currency VARCHAR(10),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CRM'
);

-- Table: crm_sales_details
CREATE TABLE crm_sales_details (
    transaction_id VARCHAR(50),
    transaction_date DATETIME,
    cust_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(18,2),
    discount_pct DECIMAL(5,2),
    tax_amount DECIMAL(18,2),
    total_amount DECIMAL(18,2),
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CRM'
);
