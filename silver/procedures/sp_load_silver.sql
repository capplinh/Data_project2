-- =============================================
-- Stored Procedure: Load Silver Layer
-- Mục đích: Clean và transform dữ liệu từ Bronze sang Silver
-- =============================================

CREATE OR REPLACE PROCEDURE sp_load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_rows_processed INT := 0;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    
    -- Log start
    INSERT INTO etl_log (procedure_name, layer, status, start_time)
    VALUES ('sp_load_silver', 'SILVER', 'STARTED', v_start_time);
    
    -- Clean CRM Customer Info
    TRUNCATE TABLE clean_crm_cust_info;
    INSERT INTO clean_crm_cust_info
    SELECT DISTINCT
        TRIM(cust_id) as cust_id,
        TRIM(UPPER(cust_name)) as cust_name,
        LOWER(TRIM(email)) as email,
        REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as phone,
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
    WHERE cust_id IS NOT NULL;
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    
    -- Clean other tables...
    -- (Similar logic for other tables)
    
    -- Log completion
    INSERT INTO etl_log (procedure_name, layer, status, end_time, rows_processed)
    VALUES ('sp_load_silver', 'SILVER', 'COMPLETED', CURRENT_TIMESTAMP, v_rows_processed);
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO etl_log (procedure_name, layer, status, error_message)
        VALUES ('sp_load_silver', 'SILVER', 'FAILED', SQLERRM);
        RAISE;
END;
$$;
