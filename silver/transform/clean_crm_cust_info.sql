-- =============================================
-- Clean CRM Customer Info
-- =============================================

TRUNCATE TABLE clean_crm_cust_info;

INSERT INTO clean_crm_cust_info
SELECT DISTINCT
    TRIM(cust_id) as cust_id,
    TRIM(UPPER(cust_name)) as cust_name,
    LOWER(TRIM(email)) as email,
    REGEXP_REPLACE(phone, '[^0-9]', '') as phone,
    TRIM(address) as full_address,
    TRIM(city) as city,
    TRIM(state) as state,
    TRIM(zip_code) as zip_code,
    registration_date,
    COALESCE(customer_type, 'UNKNOWN') as customer_type,
    COALESCE(status, 'ACTIVE') as status,
    CASE 
        WHEN email IS NOT NULL AND phone IS NOT NULL THEN 1.0
        WHEN email IS NOT NULL OR phone IS NOT NULL THEN 0.8
        ELSE 0.5
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM crm_cust_info
WHERE cust_id IS NOT NULL
  AND (email IS NULL OR email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');
