-- =============================================
-- Clean CRM Product Info
-- =============================================

TRUNCATE TABLE clean_crm_prd_info;

INSERT INTO clean_crm_prd_info
SELECT DISTINCT
    TRIM(product_id) as product_id,
    TRIM(product_name) as product_name,
    TRIM(description) as description,
    TRIM(UPPER(category)) as category,
    TRIM(UPPER(brand)) as brand,
    unit_price,
    COALESCE(currency, 'USD') as currency,
    COALESCE(status, 'ACTIVE') as status,
    CASE 
        WHEN description IS NOT NULL AND brand IS NOT NULL THEN 1.0
        WHEN description IS NOT NULL OR brand IS NOT NULL THEN 0.8
        ELSE 0.6
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM crm_prd_info
WHERE product_id IS NOT NULL
  AND unit_price >= 0;
