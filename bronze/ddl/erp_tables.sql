-- =============================================
-- Bronze Layer - ERP Tables DDL
-- Mục đích: Lưu trữ dữ liệu thô từ ERP System
-- =============================================

-- Table: erp_cust_az12
CREATE TABLE erp_cust_az12 (
    customer_code VARCHAR(50),
    customer_full_name VARCHAR(200),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(20),
    business_type VARCHAR(50),
    credit_limit DECIMAL(18,2),
    account_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'ERP'
);

-- Table: erp_loc_a101
CREATE TABLE erp_loc_a101 (
    location_id VARCHAR(50),
    location_name VARCHAR(200),
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'ERP'
);

-- Table: erp_px_cat_g1v2
CREATE TABLE erp_px_cat_g1v2 (
    product_code VARCHAR(50),
    product_desc VARCHAR(200),
    category_code VARCHAR(50),
    category_name VARCHAR(100),
    subcategory VARCHAR(100),
    unit_cost DECIMAL(18,2),
    stock_qty INT,
    reorder_level INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'ERP'
);
