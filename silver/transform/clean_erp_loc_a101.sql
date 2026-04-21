-- =============================================
-- Clean ERP Location Data
-- =============================================

TRUNCATE TABLE clean_erp_loc_a101;

INSERT INTO clean_erp_loc_a101
SELECT DISTINCT
    TRIM(location_id) as location_id,
    TRIM(location_name) as location_name,
    CONCAT_WS(', ', 
        NULLIF(TRIM(address_line1), ''),
        NULLIF(TRIM(address_line2), '')
    ) as full_address,
    TRIM(city) as city,
    TRIM(state) as state,
    TRIM(country) as country,
    TRIM(postal_code) as postal_code,
    TRIM(UPPER(region)) as region,
    CASE 
        WHEN city IS NOT NULL AND region IS NOT NULL THEN 1.0
        WHEN city IS NOT NULL OR region IS NOT NULL THEN 0.8
        ELSE 0.6
    END as data_quality_score,
    CURRENT_TIMESTAMP as processed_at
FROM bronze.erp_loc_a101
WHERE location_id IS NOT NULL;
