-- =============================================
-- Silver Layer - Cleaned Tables DDL
-- Mục đích: Lưu trữ dữ liệu đã làm sạch và validate
-- =============================================

-- Table: clean_crm_cust_info
CREATE TABLE clean_crm_cust_info (
    cust_id VARCHAR(50) PRIMARY KEY,
    cust_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    full_address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    customer_type VARCHAR(50),
    status VARCHAR(20),
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: clean_crm_prd_info
CREATE TABLE clean_crm_prd_info (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(18,2),
    currency VARCHAR(10) DEFAULT 'USD',
    status VARCHAR(20),
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: clean_crm_sales_details
CREATE TABLE clean_crm_sales_details (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    cust_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT CHECK (quantity > 0),
    unit_price DECIMAL(18,2),
    discount_pct DECIMAL(5,2) DEFAULT 0,
    tax_amount DECIMAL(18,2),
    total_amount DECIMAL(18,2),
    payment_method VARCHAR(50),
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: clean_erp_cust_az12
CREATE TABLE clean_erp_cust_az12 (
    customer_code VARCHAR(50) PRIMARY KEY,
    customer_full_name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(100),
    contact_phone VARCHAR(20),
    business_type VARCHAR(50),
    credit_limit DECIMAL(18,2),
    account_status VARCHAR(20),
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: clean_erp_loc_a101
CREATE TABLE clean_erp_loc_a101 (
    location_id VARCHAR(50) PRIMARY KEY,
    location_name VARCHAR(200) NOT NULL,
    full_address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: clean_erp_px_cat_g1v2
CREATE TABLE clean_erp_px_cat_g1v2 (
    product_code VARCHAR(50) PRIMARY KEY,
    product_desc VARCHAR(200) NOT NULL,
    category_code VARCHAR(50),
    category_name VARCHAR(100),
    subcategory VARCHAR(100),
    unit_cost DECIMAL(18,2),
    stock_qty INT,
    reorder_level INT,
    data_quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
