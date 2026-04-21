USE bronze;

-- =============================================
-- Test Bronze Layer
-- =============================================

SELECT '========================================' as '';
SELECT 'BRONZE LAYER - TEST RESULTS' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- Test 1: Row counts
SELECT 'Test 1: Row Counts' as '';
SELECT 'crm_cust_info' as table_name, COUNT(*) as row_count FROM crm_cust_info
UNION ALL SELECT 'crm_prd_info', COUNT(*) FROM crm_prd_info
UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
UNION ALL SELECT 'erp_cust_az12', COUNT(*) FROM erp_cust_az12
UNION ALL SELECT 'erp_loc_a101', COUNT(*) FROM erp_loc_a101
UNION ALL SELECT 'erp_px_cat_g1v2', COUNT(*) FROM erp_px_cat_g1v2;

SELECT '' as '';

-- Test 2: Sample CRM customers
SELECT 'Test 2: Sample CRM Customers (Top 3)' as '';
SELECT cust_id, cust_name, email, city, customer_type, status
FROM crm_cust_info
LIMIT 3;

SELECT '' as '';

-- Test 3: Sample CRM products
SELECT 'Test 3: Sample CRM Products (Top 3)' as '';
SELECT product_id, product_name, category, brand, unit_price
FROM crm_prd_info
LIMIT 3;

SELECT '' as '';

-- Test 4: Sample sales transactions
SELECT 'Test 4: Sample Sales Transactions (Top 3)' as '';
SELECT transaction_id, transaction_date, cust_id, product_id, 
       quantity, total_amount, payment_method
FROM crm_sales_details
LIMIT 3;

SELECT '' as '';
SELECT '✓ Bronze Layer Tests Completed!' as '';
