USE gold;

-- =============================================
-- Test Gold Layer
-- =============================================

SELECT '========================================' as '';
SELECT 'GOLD LAYER - TEST RESULTS' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- Test 1: Dimension counts
SELECT 'Test 1: Dimension Table Counts' as '';
SELECT 
    'dim_customers' as dimension,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
    SUM(CASE WHEN is_current THEN 0 ELSE 1 END) as historical_records
FROM dim_customers
UNION ALL
SELECT 
    'dim_products',
    COUNT(*),
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
    SUM(CASE WHEN is_current THEN 0 ELSE 1 END)
FROM dim_products
UNION ALL
SELECT 
    'dim_date',
    COUNT(*),
    COUNT(*),
    0
FROM dim_date;

SELECT '' as '';

-- Test 2: Fact table summary
SELECT 'Test 2: Fact Sales Summary' as '';
SELECT 
    COUNT(*) as total_transactions,
    SUM(quantity) as total_items_sold,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction,
    ROUND(MIN(total_amount), 2) as min_transaction,
    ROUND(MAX(total_amount), 2) as max_transaction
FROM fact_sales;

SELECT '' as '';

-- Test 3: Sample dimension data
SELECT 'Test 3: Sample Customers (Current)' as '';
SELECT customer_id, customer_name, city, region, is_current
FROM dim_customers
WHERE is_current = TRUE
LIMIT 3;

SELECT '' as '';

-- Test 4: Sample products
SELECT 'Test 4: Sample Products (Current)' as '';
SELECT product_id, product_name, category, brand, is_current
FROM dim_products
WHERE is_current = TRUE
LIMIT 3;

SELECT '' as '';
SELECT '✓ Gold Layer Tests Completed!' as '';
