-- =============================================================
-- silver_transform.sql
-- Tầng SILVER — Làm sạch & chuẩn hoá dữ liệu từ Bronze
-- Chạy: psql -U erp_user -d erp_source -f silver_transform.sql
--
-- Thứ tự chạy:
--   1. Tạo schema + helper function
--   2. silver.users
--   3. silver.products
--   4. silver.orders
--   5. silver.order_items
--   6. silver.transactions
--   7. silver.data_quality_report  (báo cáo chất lượng)
-- =============================================================


-- ════════════════════════════════════════════════════════════
-- 0. SETUP
-- ════════════════════════════════════════════════════════════
-- Idempotent: drop and recreate silver schema cleanly
DROP SCHEMA IF EXISTS silver CASCADE;
CREATE SCHEMA silver;
CREATE SCHEMA IF NOT EXISTS silver;

-- Helper: chuẩn hoá nhiều định dạng ngày → DATE
-- Xử lý: yyyy-mm-dd | dd/mm/yyyy | mm-dd-yyyy | yyyymmdd | dd Mon yyyy
CREATE OR REPLACE FUNCTION silver.parse_date(raw TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result DATE;
BEGIN
    IF raw IS NULL OR TRIM(raw) = '' OR UPPER(TRIM(raw)) = 'N/A' THEN
        RETURN NULL;
    END IF;

    -- Thử lần lượt các format phổ biến
    BEGIN result := raw::DATE;                           RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN result := TO_DATE(raw, 'DD/MM/YYYY');          RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN result := TO_DATE(raw, 'MM-DD-YYYY');          RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN result := TO_DATE(raw, 'YYYYMMDD');            RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN result := TO_DATE(raw, 'DD Mon YYYY');         RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN result := TO_DATE(raw, 'DD Month YYYY');       RETURN result; EXCEPTION WHEN OTHERS THEN NULL; END;

    RETURN NULL;  -- không parse được → NULL, ghi nhận vào DQ report
END;
$$;

-- Helper: validate email đơn giản
CREATE OR REPLACE FUNCTION silver.is_valid_email(email TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT email ~ '^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$';
$$;

-- Helper: chuẩn hoá phone VN (giữ 10 số, bỏ +84 / 084 / 84)
CREATE OR REPLACE FUNCTION silver.normalize_phone(phone TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF phone IS NULL THEN RETURN NULL; END IF;
    cleaned := REGEXP_REPLACE(phone, '[^0-9]', '', 'g');   -- bỏ ký tự không phải số
    IF cleaned LIKE '84%' AND LENGTH(cleaned) = 11 THEN
        cleaned := '0' || SUBSTRING(cleaned FROM 3);       -- 84xxx → 0xxx
    END IF;
    IF LENGTH(cleaned) = 10 AND cleaned LIKE '0%' THEN
        RETURN cleaned;
    END IF;
    RETURN NULL;   -- không hợp lệ
END;
$$;


-- ════════════════════════════════════════════════════════════
-- 1. SILVER.USERS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS silver.users;

CREATE TABLE silver.users AS
WITH

-- B1: Lấy batch mới nhất từ Bronze (tránh count nhiều lần khi Bronze append)
latest_batch AS (
    SELECT MAX(_ingested_at) AS max_ts FROM bronze.users
),

-- B2: Chỉ lấy batch mới nhất
raw AS (
    SELECT b.*
    FROM bronze.users b
    JOIN latest_batch lb ON b._ingested_at = lb.max_ts
),

-- B3: DEDUP — giữ 1 row mỗi email, ưu tiên row có nhiều thông tin nhất
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(email))       -- cùng email = duplicate
            ORDER BY
                (phone    IS NOT NULL) DESC,       -- ưu tiên row có phone
                (city     IS NOT NULL) DESC,
                (country  IS NOT NULL) DESC,
                _bronze_id DESC                    -- tie-break: row mới nhất
        ) AS rn
    FROM raw
),

-- B4: Transform & validate
cleaned AS (
    SELECT
        user_id::INTEGER                                            AS user_id,

        -- Tên: trim khoảng trắng, title case
        INITCAP(TRIM(full_name))                                    AS full_name,

        -- Email: lowercase, NULL nếu sai format
        CASE
            WHEN silver.is_valid_email(LOWER(TRIM(email)))
            THEN LOWER(TRIM(email))
            ELSE NULL
        END                                                         AS email,

        -- Phone: chuẩn hoá
        silver.normalize_phone(phone)                               AS phone,

        -- City / Country: trim, COALESCE về 'Unknown'
        COALESCE(NULLIF(TRIM(city),    ''), 'Unknown')              AS city,
        COALESCE(NULLIF(TRIM(country), ''), 'Unknown')              AS country,

        -- Date: parse nhiều format → DATE chuẩn
        silver.parse_date(created_at)                               AS created_date,

        -- Boolean: xử lý khi Bronze lưu dạng text 'true'/'false'
        CASE
            WHEN LOWER(is_active) IN ('true','t','1','yes') THEN TRUE
            WHEN LOWER(is_active) IN ('false','f','0','no') THEN FALSE
            ELSE NULL
        END                                                         AS is_active,

        -- Metadata Silver
        NOW()                                                       AS _silver_processed_at,
        _batch_id                                                   AS _source_batch_id,

        -- Flag chất lượng (dùng cho DQ report)
        CASE WHEN NOT silver.is_valid_email(LOWER(TRIM(email))) THEN 1 ELSE 0 END
            AS _flag_bad_email,
        CASE WHEN silver.parse_date(created_at) IS NULL THEN 1 ELSE 0 END
            AS _flag_bad_date,
        CASE WHEN silver.normalize_phone(phone) IS NULL AND phone IS NOT NULL THEN 1 ELSE 0 END
            AS _flag_bad_phone

    FROM deduped
    WHERE rn = 1   -- bỏ duplicate
)

SELECT * FROM cleaned;

-- Index thường dùng
CREATE INDEX idx_silver_users_user_id ON silver.users(user_id);
CREATE INDEX idx_silver_users_email   ON silver.users(email);

SELECT 'silver.users' AS tbl, COUNT(*) AS rows FROM silver.users;


-- ════════════════════════════════════════════════════════════
-- 2. SILVER.PRODUCTS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS silver.products;

CREATE TABLE silver.products AS
WITH
latest_batch AS (SELECT MAX(_ingested_at) AS max_ts FROM bronze.products),
raw AS (
    SELECT b.* FROM bronze.products b
    JOIN latest_batch lb ON b._ingested_at = lb.max_ts
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY _bronze_id DESC
        ) AS rn
    FROM raw
),
cleaned AS (
    SELECT
        product_id::INTEGER                                         AS product_id,
        TRIM(product_name)                                          AS product_name,
        COALESCE(NULLIF(TRIM(category),    ''), 'Uncategorized')    AS category,
        COALESCE(NULLIF(TRIM(subcategory), ''), 'Other')            AS subcategory,

        -- Giá: lọc âm → NULL (sẽ impute bằng AVG category ở Gold nếu cần)
        CASE WHEN unit_price::NUMERIC > 0 THEN unit_price::NUMERIC ELSE NULL END
            AS unit_price,
        CASE WHEN cost_price::NUMERIC > 0 THEN cost_price::NUMERIC ELSE NULL END
            AS cost_price,

        -- Stock: âm → 0
        GREATEST(COALESCE(stock_qty::INTEGER, 0), 0)                AS stock_qty,

        COALESCE(NULLIF(TRIM(supplier), ''), 'Unknown')             AS supplier,

        NOW()           AS _silver_processed_at,
        _batch_id       AS _source_batch_id,

        CASE WHEN unit_price::NUMERIC <= 0 THEN 1 ELSE 0 END        AS _flag_negative_price

    FROM deduped
    WHERE rn = 1
)
SELECT * FROM cleaned;

CREATE INDEX idx_silver_products_id ON silver.products(product_id);

SELECT 'silver.products' AS tbl, COUNT(*) AS rows FROM silver.products;


-- ════════════════════════════════════════════════════════════
-- 3. SILVER.ORDERS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS silver.orders;

CREATE TABLE silver.orders AS
WITH
latest_batch AS (SELECT MAX(_ingested_at) AS max_ts FROM bronze.orders),
raw AS (
    SELECT b.* FROM bronze.orders b
    JOIN latest_batch lb ON b._ingested_at = lb.max_ts
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _bronze_id DESC
        ) AS rn
    FROM raw
),
cleaned AS (
    SELECT
        order_id::INTEGER                                           AS order_id,
        user_id::INTEGER                                            AS user_id,

        -- Ngày: parse nhiều format
        silver.parse_date(order_date)                               AS order_date,

        -- Status: lowercase, chuẩn hoá typo thường gặp
        CASE LOWER(TRIM(status))
            WHEN 'peding'    THEN 'pending'
            WHEN 'shiped'    THEN 'shipped'
            WHEN 'deliverd'  THEN 'delivered'
            WHEN 'canceld'   THEN 'cancelled'
            ELSE LOWER(TRIM(status))
        END                                                         AS status,

        COALESCE(NULLIF(TRIM(shipping_city), ''), 'Unknown')        AS shipping_city,

        -- Amount: lọc âm, NULL nếu không hợp lệ
        CASE WHEN total_amount::NUMERIC > 0 THEN total_amount::NUMERIC ELSE NULL END
            AS total_amount,

        -- Discount: clamp về [0, 100]
        CASE
            WHEN discount IS NULL THEN 0
            WHEN discount::NUMERIC < 0 THEN 0
            WHEN discount::NUMERIC > 100 THEN 100
            ELSE ROUND(discount::NUMERIC, 2)
        END                                                         AS discount_pct,

        NOW()           AS _silver_processed_at,
        _batch_id       AS _source_batch_id,

        CASE WHEN silver.parse_date(order_date) IS NULL THEN 1 ELSE 0 END
            AS _flag_bad_date,
        CASE WHEN total_amount::NUMERIC <= 0 THEN 1 ELSE 0 END
            AS _flag_negative_amount

    FROM deduped
    WHERE rn = 1
      AND user_id IS NOT NULL     -- bỏ orphan orders
)
SELECT * FROM cleaned;

CREATE INDEX idx_silver_orders_id      ON silver.orders(order_id);
CREATE INDEX idx_silver_orders_user_id ON silver.orders(user_id);
CREATE INDEX idx_silver_orders_date    ON silver.orders(order_date);

SELECT 'silver.orders' AS tbl, COUNT(*) AS rows FROM silver.orders;


-- ════════════════════════════════════════════════════════════
-- 4. SILVER.ORDER_ITEMS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS silver.order_items;

CREATE TABLE silver.order_items AS
WITH
latest_batch AS (SELECT MAX(_ingested_at) AS max_ts FROM bronze.order_items),
raw AS (
    SELECT b.* FROM bronze.order_items b
    JOIN latest_batch lb ON b._ingested_at = lb.max_ts
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY _bronze_id DESC
        ) AS rn
    FROM raw
),
cleaned AS (
    SELECT
        item_id::INTEGER                                            AS item_id,
        order_id::INTEGER                                           AS order_id,
        product_id::INTEGER                                         AS product_id,

        GREATEST(quantity::INTEGER, 1)                              AS quantity,
        GREATEST(unit_price::NUMERIC, 0)                            AS unit_price,

        -- Tính lại subtotal thay vì tin vào source (có thể sai)
        GREATEST(quantity::INTEGER, 1) * GREATEST(unit_price::NUMERIC, 0)
            AS subtotal_calculated,

        NOW()           AS _silver_processed_at,
        _batch_id       AS _source_batch_id

    FROM deduped
    WHERE rn = 1
      AND order_id IS NOT NULL
      AND product_id IS NOT NULL
      -- Chỉ giữ order_items có order hợp lệ ở Silver
      AND order_id::INTEGER IN (SELECT order_id FROM silver.orders)
)
SELECT * FROM cleaned;

CREATE INDEX idx_silver_items_order_id   ON silver.order_items(order_id);
CREATE INDEX idx_silver_items_product_id ON silver.order_items(product_id);

SELECT 'silver.order_items' AS tbl, COUNT(*) AS rows FROM silver.order_items;


-- ════════════════════════════════════════════════════════════
-- 5. SILVER.TRANSACTIONS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS silver.transactions;

CREATE TABLE silver.transactions AS
WITH
latest_batch AS (SELECT MAX(_ingested_at) AS max_ts FROM bronze.transactions),
raw AS (
    SELECT b.* FROM bronze.transactions b
    JOIN latest_batch lb ON b._ingested_at = lb.max_ts
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY txn_id
            ORDER BY _bronze_id DESC
        ) AS rn
    FROM raw
),
cleaned AS (
    SELECT
        txn_id::INTEGER                                             AS txn_id,
        order_id::INTEGER                                           AS order_id,
        user_id::INTEGER                                            AS user_id,

        silver.parse_date(txn_date)                                 AS txn_date,

        -- Amount: chỉ giữ dương
        CASE WHEN amount::NUMERIC > 0 THEN amount::NUMERIC ELSE NULL END
            AS amount,

        LOWER(TRIM(payment_method))                                 AS payment_method,

        LOWER(TRIM(status))                                         AS status,

        -- Currency: standardize
        CASE UPPER(TRIM(currency))
            WHEN 'VN'  THEN 'VND'
            WHEN 'US'  THEN 'USD'
            ELSE UPPER(TRIM(currency))
        END                                                         AS currency,

        NOW()           AS _silver_processed_at,
        _batch_id       AS _source_batch_id,

        CASE WHEN silver.parse_date(txn_date) IS NULL THEN 1 ELSE 0 END
            AS _flag_bad_date,
        CASE WHEN amount::NUMERIC <= 0 THEN 1 ELSE 0 END
            AS _flag_negative_amount

    FROM deduped
    WHERE rn = 1
)
SELECT * FROM cleaned;

CREATE INDEX idx_silver_txn_id       ON silver.transactions(txn_id);
CREATE INDEX idx_silver_txn_order_id ON silver.transactions(order_id);
CREATE INDEX idx_silver_txn_date     ON silver.transactions(txn_date);

SELECT 'silver.transactions' AS tbl, COUNT(*) AS rows FROM silver.transactions;


-- ════════════════════════════════════════════════════════════
-- 6. DATA QUALITY REPORT
-- Tổng hợp % lỗi trước & sau làm sạch — dùng khi trình bày
-- ════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS silver.vw_data_quality_report;

CREATE VIEW silver.vw_data_quality_report AS

SELECT 'users' AS entity,
    (SELECT COUNT(*) FROM bronze.users)                             AS bronze_count,
    (SELECT COUNT(*) FROM silver.users)                             AS silver_count,
    (SELECT COUNT(*) FROM silver.users WHERE _flag_bad_email = 1)   AS bad_email,
    (SELECT COUNT(*) FROM silver.users WHERE _flag_bad_date  = 1)   AS bad_date,
    (SELECT COUNT(*) FROM silver.users WHERE _flag_bad_phone = 1)   AS bad_phone,
    NULL::BIGINT                                                    AS bad_amount

UNION ALL

SELECT 'products',
    (SELECT COUNT(*) FROM bronze.products),
    (SELECT COUNT(*) FROM silver.products),
    NULL, NULL, NULL,
    (SELECT COUNT(*) FROM silver.products WHERE _flag_negative_price = 1)

UNION ALL

SELECT 'orders',
    (SELECT COUNT(*) FROM bronze.orders),
    (SELECT COUNT(*) FROM silver.orders),
    NULL,
    (SELECT COUNT(*) FROM silver.orders WHERE _flag_bad_date      = 1),
    NULL,
    (SELECT COUNT(*) FROM silver.orders WHERE _flag_negative_amount = 1)

UNION ALL

SELECT 'transactions',
    (SELECT COUNT(*) FROM bronze.transactions),
    (SELECT COUNT(*) FROM silver.transactions),
    NULL,
    (SELECT COUNT(*) FROM silver.transactions WHERE _flag_bad_date       = 1),
    NULL,
    (SELECT COUNT(*) FROM silver.transactions WHERE _flag_negative_amount = 1);

-- Xem báo cáo ngay
SELECT * FROM silver.vw_data_quality_report;