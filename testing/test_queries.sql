-- =============================================
-- Star Schema Test Queries - MySQL
-- =============================================

SELECT '========================================' as '';
SELECT 'STAR SCHEMA - ANALYTICAL QUERIES' as '';
SELECT '========================================' as '';
SELECT '' as '';

-- Query 1: Top 5 Customers by Revenue
SELECT 'Query 1: Top 5 Customers by Revenue' as '';
SELECT 
    c.customer_name,
    c.city,
    c.region,
    COUNT(f.sales_key) as total_purchases,
    ROUND(SUM(f.total_amount), 2) as total_spent
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_name, c.city, c.region
ORDER BY total_spent DESC
LIMIT 5;

SELECT '' as '';

-- Query 2: Top 5 Products by Revenue
SELECT 'Query 2: Top 5 Products by Revenue' as '';
SELECT 
    p.product_name,
    p.category,
    p.brand,
    COUNT(f.sales_key) as times_sold,
    SUM(f.quantity) as total_quantity,
    ROUND(SUM(f.total_amount), 2) as total_revenue
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category, p.brand
ORDER BY total_revenue DESC
LIMIT 5;

SELECT '' as '';

-- Query 3: Monthly Sales Trend
SELECT 'Query 3: Monthly Sales Trend' as '';
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.sales_key) as transaction_count,
    ROUND(SUM(f.total_amount), 2) as monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

SELECT '' as '';

-- Query 4: Sales by Region
SELECT 'Query 4: Sales by Region' as '';
SELECT 
    c.region,
    COUNT(DISTINCT c.customer_key) as unique_customers,
    COUNT(f.sales_key) as total_transactions,
    ROUND(SUM(f.total_amount), 2) as total_revenue,
    ROUND(AVG(f.total_amount), 2) as avg_transaction_value
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.region
ORDER BY total_revenue DESC;

SELECT '' as '';

-- Query 5: Product Category Performance
SELECT 'Query 5: Product Category Performance' as '';
SELECT 
    p.category,
    COUNT(DISTINCT p.product_key) as product_count,
    COUNT(f.sales_key) as total_sales,
    SUM(f.quantity) as units_sold,
    ROUND(SUM(f.total_amount), 2) as revenue,
    ROUND(AVG(f.total_amount), 2) as avg_sale_value
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category
ORDER BY revenue DESC;

SELECT '' as '';

-- Query 6: Customer Segmentation
SELECT 'Query 6: Customer Segmentation by Type' as '';
SELECT 
    c.customer_type,
    COUNT(DISTINCT c.customer_key) as customer_count,
    COUNT(f.sales_key) as total_purchases,
    ROUND(AVG(f.total_amount), 2) as avg_purchase_value,
    ROUND(SUM(f.total_amount), 2) as total_revenue
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_type
ORDER BY total_revenue DESC;

SELECT '' as '';

-- Query 7: Payment Method Analysis
SELECT 'Query 7: Payment Method Analysis' as '';
SELECT 
    f.payment_method,
    COUNT(f.sales_key) as transaction_count,
    ROUND(SUM(f.total_amount), 2) as total_amount,
    ROUND(AVG(f.total_amount), 2) as avg_amount
FROM fact_sales f
GROUP BY f.payment_method
ORDER BY total_amount DESC;

SELECT '' as '';

-- Query 8: Weekend vs Weekday Sales
SELECT 'Query 8: Weekend vs Weekday Sales' as '';
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(f.sales_key) as transactions,
    ROUND(SUM(f.total_amount), 2) as revenue,
    ROUND(AVG(f.total_amount), 2) as avg_transaction
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY day_type;

SELECT '' as '';
SELECT '✓ All Star Schema Queries Completed!' as '';
