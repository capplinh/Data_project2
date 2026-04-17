-- =============================================
-- Stored Procedure: Load Bronze Layer
-- Mục đích: Load dữ liệu từ source systems vào Bronze
-- =============================================

CREATE OR REPLACE PROCEDURE sp_load_bronze(
    p_load_type VARCHAR(20) DEFAULT 'FULL'  -- FULL or INCREMENTAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_rows_loaded INT;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    
    -- Log start
    INSERT INTO etl_log (procedure_name, layer, status, start_time)
    VALUES ('sp_load_bronze', 'BRONZE', 'STARTED', v_start_time);
    
    -- Load CRM tables
    -- TODO: Implement actual data loading from source
    -- Example: COPY or INSERT INTO SELECT from external sources
    
    -- Load ERP tables
    -- TODO: Implement actual data loading from source
    
    -- Log completion
    INSERT INTO etl_log (procedure_name, layer, status, end_time, rows_processed)
    VALUES ('sp_load_bronze', 'BRONZE', 'COMPLETED', CURRENT_TIMESTAMP, v_rows_loaded);
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO etl_log (procedure_name, layer, status, error_message)
        VALUES ('sp_load_bronze', 'BRONZE', 'FAILED', SQLERRM);
        RAISE;
END;
$$;
