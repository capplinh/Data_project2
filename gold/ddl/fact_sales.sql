-- =============================================
-- Gold Layer - Fact Sales
-- Mục đích: Fact table cho dữ liệu bán hàng
-- =============================================

CREATE TABLE fact_sales (
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Foreign Keys to Dimensions
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    
    -- Measures (Metrics)
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) DEFAULT 0,
    tax_amount DECIMAL(18,2) DEFAULT 0,
    total_amount DECIMAL(18,2) NOT NULL,
    
    -- Degenerate Dimensions
    payment_method VARCHAR(50),
    
    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) 
        REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) 
        REFERENCES dim_customers(customer_key),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) 
        REFERENCES dim_products(product_key),
    
    -- Indexes for query performance
    INDEX idx_fact_sales_date (date_key),
    INDEX idx_fact_sales_customer (customer_key),
    INDEX idx_fact_sales_product (product_key),
    INDEX idx_fact_sales_date_customer (date_key, customer_key),
    INDEX idx_fact_sales_date_product (date_key, product_key)
);
