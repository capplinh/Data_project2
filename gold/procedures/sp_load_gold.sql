-- =============================================
-- Stored Procedure: Load Gold Layer
-- Mục đích: Load và transform dữ liệu từ Silver sang Gold
-- =============================================

CREATE OR REPLACE PROCEDURE sp_load_gold()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_rows_processed INT := 0;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    
    -- Log start
    INSERT INTO etl_log (procedure_name, layer, status, start_time)
    VALUES ('sp_load_gold', 'GOLD', 'STARTED', v_start_time);
    
    -- Load Dimension Customers (SCD Type 2)
    -- Detect changes and create new records with effective dates
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
        CURRENT_DATE as effective_date,
        TRUE as is_current
    FROM clean_crm_cust_info c
    FULL OUTER JOIN clean_erp_cust_az12 e 
        ON c.cust_id = e.customer_code
    LEFT JOIN clean_erp_loc_a101 l 
        ON c.city = l.city
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customers d
        WHERE d.customer_id = COALESCE(c.cust_id, e.customer_code)
        AND d.is_current = TRUE
    );
    
    -- Load Dimension Products (SCD Type 2)
    INSERT INTO dim_products (
        product_id, product_name, description, category, subcategory,
        brand, unit_price, unit_cost, currency, status,
        effective_date, is_current
    )
    SELECT DISTINCT
        COALESCE(p.product_id, e.product_code) as product_id,
        COALESCE(p.product_name, e.product_desc) as product_name,
        p.description,
        COALESCE(p.category, e.category_name) as category,
        e.subcategory,
        p.brand,
        p.unit_price,
        e.unit_cost,
        p.currency,
        p.status,
        CURRENT_DATE as effective_date,
        TRUE as is_current
    FROM clean_crm_prd_info p
    FULL OUTER JOIN clean_erp_px_cat_g1v2 e 
        ON p.product_id = e.product_code
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_products d
        WHERE d.product_id = COALESCE(p.product_id, e.product_code)
        AND d.is_current = TRUE
    );
    
    -- Load Fact Sales
    INSERT INTO fact_sales (
        transaction_id, date_key, customer_key, product_key,
        quantity, unit_price, discount_amount, tax_amount,
        total_amount, payment_method
    )
    SELECT 
        s.transaction_id,
        TO_CHAR(s.transaction_date, 'YYYYMMDD')::INT as date_key,
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
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    
    -- Log completion
    INSERT INTO etl_log (procedure_name, layer, status, end_time, rows_processed)
    VALUES ('sp_load_gold', 'GOLD', 'COMPLETED', CURRENT_TIMESTAMP, v_rows_processed);
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO etl_log (procedure_name, layer, status, error_message)
        VALUES ('sp_load_gold', 'GOLD', 'FAILED', SQLERRM);
        RAISE;
END;
$$;
