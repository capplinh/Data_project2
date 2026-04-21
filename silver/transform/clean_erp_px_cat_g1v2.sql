-- =============================================
-- Clean ERP Product Catalog
-- =============================================

TRUNCATE TABLE clean_erp_px_cat_g1v2;

INSERT INTO clean_erp_px_cat_g1v2
SELECT DISTINCT
    TRIM(product_code) as product_code,
    TRIM(product_desc) as product_desc,
    TRIM(category_code) as category_code,
    TRIM(UPPER(category_name)) as category_name,
    TRIM(UPPER(subcategory)) as subcategory,
    COALESCE(unit_cost, 0) as unit_cost,
    COALESCE(stock_qty, 0) as stock_qty,
    COALESCE(reorder_level, 0) as reorder_level,
    CASE 
        WHEN category_name IS NOT NULL AND unit_cost > 0 THEN 1.0
        WHEN category_name IS NOT NULL OR unit_cost > 0 THEN 0.8
        ELSE 0.6
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM bronze.erp_px_cat_g1v2
WHERE product_code IS NOT NULL;
