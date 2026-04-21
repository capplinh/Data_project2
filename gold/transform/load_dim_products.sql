-- =============================================
-- Load Dimension Products (SCD Type 2) - MySQL
-- =============================================

-- Step 1: Bỏ qua SCD Type 2 update cho đơn giản

-- Step 2: Insert new records
INSERT INTO dim_products (
    product_id, product_name, description, category, subcategory,
    brand, unit_price, unit_cost, currency, status,
    effective_date, is_current
)
SELECT DISTINCT
    COALESCE(p.product_id, e.product_code) as product_id,
    COALESCE(p.product_name, e.product_desc) as product_name,
    p.description,
    COALESCE(p.category, e.category_name) as category,
    e.subcategory,
    p.brand,
    p.unit_price,
    e.unit_cost,
    COALESCE(p.currency, 'VND') as currency,
    COALESCE(p.status, 'ACTIVE') as status,
    CURDATE() as effective_date,
    TRUE as is_current
FROM silver.clean_crm_prd_info p
LEFT JOIN silver.clean_erp_px_cat_g1v2 e ON p.product_id = e.product_code
WHERE NOT EXISTS (
    SELECT 1 FROM dim_products d
    WHERE d.product_id = p.product_id
)
UNION
SELECT DISTINCT
    e.product_code as product_id,
    e.product_desc as product_name,
    NULL as description,
    e.category_name as category,
    e.subcategory,
    NULL as brand,
    NULL as unit_price,
    e.unit_cost,
    'VND' as currency,
    'ACTIVE' as status,
    CURDATE() as effective_date,
    TRUE as is_current
FROM silver.clean_erp_px_cat_g1v2 e
LEFT JOIN silver.clean_crm_prd_info p ON e.product_code = p.product_id
WHERE p.product_id IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM dim_products d
    WHERE d.product_id = e.product_code
);
