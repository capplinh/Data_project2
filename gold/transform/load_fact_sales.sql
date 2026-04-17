-- =============================================
-- Load Fact Sales
-- =============================================

INSERT INTO fact_sales (
    transaction_id, date_key, customer_key, product_key,
    quantity, unit_price, discount_amount, tax_amount,
    total_amount, payment_method
)
SELECT 
    s.transaction_id,
    DATE_FORMAT(s.transaction_date, '%Y%m%d') as date_key,
    dc.customer_key,
    dp.product_key,
    s.quantity,
    s.unit_price,
    (s.unit_price * s.quantity * s.discount_pct / 100) as discount_amount,
    s.tax_amount,
    s.total_amount,
    s.payment_method
FROM clean_crm_sales_details s
INNER JOIN dim_customers dc 
    ON s.cust_id = dc.customer_id AND dc.is_current = TRUE
INNER JOIN dim_products dp 
    ON s.product_id = dp.product_id AND dp.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM fact_sales f
    WHERE f.transaction_id = s.transaction_id
);
