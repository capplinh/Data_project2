-- =============================================
-- Gold Layer - Dimension Products
-- Mục đích: Dimension table cho sản phẩm (SCD Type 2)
-- =============================================

CREATE TABLE dim_products (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(18,2),
    unit_cost DECIMAL(18,2),
    currency VARCHAR(10) DEFAULT 'USD',
    status VARCHAR(20),
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes
    CONSTRAINT uk_product_id_effective UNIQUE (product_id, effective_date),
    INDEX idx_dim_products_id (product_id),
    INDEX idx_dim_products_current (is_current),
    INDEX idx_dim_products_category (category),
    INDEX idx_dim_products_brand (brand)
);
