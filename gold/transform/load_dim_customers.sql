-- =============================================
-- Load Dimension Customers (SCD Type 2) - MySQL
-- =============================================

-- Step 1: Bỏ qua SCD Type 2 update cho đơn giản (chỉ insert new records)

-- Step 2: Insert new records
INSERT INTO dim_customers (
    customer_id, customer_name, email, phone, full_address,
    city, state, zip_code, region, customer_type, business_type,
    credit_limit, registration_date, status, effective_date, is_current
)
SELECT DISTINCT
    COALESCE(c.cust_id, e.customer_code) as customer_id,
    COALESCE(c.cust_name, e.customer_full_name) as customer_name,
    COALESCE(c.email, e.contact_email) as email,
    COALESCE(c.phone, e.contact_phone) as phone,
    c.full_address,
    c.city,
    c.state,
    c.zip_code,
    l.region,
    c.customer_type,
    e.business_type,
    e.credit_limit,
    c.registration_date,
    COALESCE(c.status, e.account_status) as status,
    CURDATE() as effective_date,
    TRUE as is_current
FROM clean_crm_cust_info c
LEFT JOIN clean_erp_cust_az12 e ON c.cust_id = e.customer_code
LEFT JOIN clean_erp_loc_a101 l ON c.city = l.city
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customers d
    WHERE d.customer_id = c.cust_id
)
UNION
SELECT DISTINCT
    e.customer_code as customer_id,
    e.customer_full_name as customer_name,
    e.contact_email as email,
    e.contact_phone as phone,
    NULL as full_address,
    NULL as city,
    NULL as state,
    NULL as zip_code,
    NULL as region,
    NULL as customer_type,
    e.business_type,
    e.credit_limit,
    NULL as registration_date,
    e.account_status as status,
    CURDATE() as effective_date,
    TRUE as is_current
FROM clean_erp_cust_az12 e
LEFT JOIN clean_crm_cust_info c ON e.customer_code = c.cust_id
WHERE c.cust_id IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM dim_customers d
    WHERE d.customer_id = e.customer_code
);
