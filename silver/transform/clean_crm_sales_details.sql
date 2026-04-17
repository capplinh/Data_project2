-- =============================================
-- Clean CRM Sales Details
-- =============================================

-- Delete existing data
DELETE FROM clean_crm_sales_details;

INSERT INTO clean_crm_sales_details
SELECT DISTINCT
    TRIM(transaction_id) as transaction_id,
    DATE(transaction_date) as transaction_date,
    TRIM(cust_id) as cust_id,
    TRIM(product_id) as product_id,
    quantity,
    unit_price,
    COALESCE(discount_pct, 0) as discount_pct,
    COALESCE(tax_amount, 0) as tax_amount,
    total_amount,
    TRIM(payment_method) as payment_method,
    CASE 
        WHEN cust_id IS NOT NULL AND product_id IS NOT NULL 
             AND quantity > 0 AND total_amount > 0 THEN 1.0
        WHEN cust_id IS NOT NULL AND product_id IS NOT NULL THEN 0.8
        ELSE 0.5
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM crm_sales_details
WHERE transaction_id IS NOT NULL
  AND quantity > 0
  AND total_amount >= 0;
