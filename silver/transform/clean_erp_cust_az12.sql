-- =============================================
-- Clean ERP Customer Data
-- =============================================

TRUNCATE TABLE clean_erp_cust_az12;

INSERT INTO clean_erp_cust_az12
SELECT DISTINCT
    TRIM(customer_code) as customer_code,
    TRIM(UPPER(customer_full_name)) as customer_full_name,
    LOWER(TRIM(contact_email)) as contact_email,
    REGEXP_REPLACE(contact_phone, '[^0-9]', '') as contact_phone,
    TRIM(business_type) as business_type,
    COALESCE(credit_limit, 0) as credit_limit,
    COALESCE(account_status, 'ACTIVE') as account_status,
    CASE 
        WHEN contact_email IS NOT NULL AND contact_phone IS NOT NULL THEN 1.0
        WHEN contact_email IS NOT NULL OR contact_phone IS NOT NULL THEN 0.8
        ELSE 0.6
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM bronze.erp_cust_az12
WHERE customer_code IS NOT NULL;
