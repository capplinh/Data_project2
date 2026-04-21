USE silver;

-- =============================================
-- Test Silver Layer
-- =============================================

SELECT '========================================' as '';
SELECT 'SILVER LAYER - TEST RESULTS' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- Test 1: Row counts và quality scores
SELECT 'Test 1: Row Counts & Quality Scores' as '';
SELECT 
    'clean_crm_cust_info' as table_name,
    COUNT(*) as row_count,
    ROUND(AVG(data_quality_score), 2) as avg_quality
FROM clean_crm_cust_info
UNION ALL
SELECT 'clean_crm_prd_info', COUNT(*), ROUND(AVG(data_quality_score), 2)
FROM clean_crm_prd_info
UNION ALL
SELECT 'clean_crm_sales_details', COUNT(*), ROUND(AVG(data_quality_score), 2)
FROM clean_crm_sales_details;

SELECT '' as '';

-- Test 2: Quality distribution
SELECT 'Test 2: Data Quality Distribution' as '';
SELECT 
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent (>=0.9)'
        WHEN data_quality_score >= 0.7 THEN 'Good (0.7-0.9)'
        WHEN data_quality_score >= 0.5 THEN 'Fair (0.5-0.7)'
        ELSE 'Poor (<0.5)'
    END as quality_level,
    COUNT(*) as record_count
FROM clean_crm_cust_info
GROUP BY quality_level
ORDER BY MIN(data_quality_score) DESC;

SELECT '' as '';

-- Test 3: Sample cleaned data
SELECT 'Test 3: Sample Cleaned Customers' as '';
SELECT cust_id, cust_name, email, city, 
       ROUND(data_quality_score, 2) as quality_score
FROM clean_crm_cust_info
LIMIT 3;

SELECT '' as '';
SELECT '✓ Silver Layer Tests Completed!' as '';
