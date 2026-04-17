-- =============================================================
-- gold_transform.sql
-- Tầng GOLD — Star Schema + SCD Type 2
-- Chạy: psql -U erp_user -d erp_source -f gold_transform.sql
--
-- Schema:
--   Dimensions : dim_date, dim_users (SCD2), dim_products
--   Facts      : fact_orders, fact_transactions
-- =============================================================


-- ════════════════════════════════════════════════════════════
-- 0. SETUP
-- ════════════════════════════════════════════════════════════
-- Idempotent: drop and recreate gold schema cleanly  
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;
CREATE SCHEMA IF NOT EXISTS gold;


-- ════════════════════════════════════════════════════════════
-- 1. DIM_DATE  — Date dimension (2020–2030)
--    Không lấy từ Silver, tự generate
--    Quan trọng cho BI: drill down year → quarter → month → day
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE gold.dim_date (
    date_key        INTEGER     PRIMARY KEY,   -- YYYYMMDD, dùng làm FK ở fact
    full_date       DATE        NOT NULL,
    day_of_week     SMALLINT,                  -- 1=Mon ... 7=Sun
    day_name        TEXT,                      -- 'Monday'
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month_num       SMALLINT,
    month_name      TEXT,
    quarter         SMALLINT,
    quarter_name    TEXT,                      -- 'Q1'
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_leap_year    BOOLEAN
);

INSERT INTO gold.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER             AS date_key,
    d                                           AS full_date,
    EXTRACT(ISODOW FROM d)::SMALLINT            AS day_of_week,
    TO_CHAR(d, 'Day')                           AS day_name,
    EXTRACT(DAY   FROM d)::SMALLINT             AS day_of_month,
    EXTRACT(DOY   FROM d)::SMALLINT             AS day_of_year,
    EXTRACT(WEEK  FROM d)::SMALLINT             AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT             AS month_num,
    TO_CHAR(d, 'Month')                         AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT           AS quarter,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT        AS quarter_name,
    EXTRACT(YEAR  FROM d)::SMALLINT             AS year,
    EXTRACT(ISODOW FROM d) IN (6, 7)            AS is_weekend,
    (EXTRACT(YEAR FROM d)::INTEGER % 4 = 0
     AND (EXTRACT(YEAR FROM d)::INTEGER % 100 != 0
          OR EXTRACT(YEAR FROM d)::INTEGER % 400 = 0))
                                                AS is_leap_year
FROM GENERATE_SERIES('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d;

SELECT 'gold.dim_date' AS tbl, COUNT(*) AS rows FROM gold.dim_date;


-- ════════════════════════════════════════════════════════════
-- 2. DIM_USERS — SCD Type 2
--
-- SCD Type 2 = mỗi lần user thay đổi thông tin (city, email...)
-- → tạo thêm 1 row mới, row cũ đóng lại
-- → giữ được lịch sử đầy đủ để phân tích "user lúc đó ở đâu"
--
-- Cột quan trọng:
--   user_sk      : surrogate key (PK của dim)
--   user_id      : natural key (ID từ source)
--   effective_from / effective_to : hiệu lực
--   is_current   : TRUE = record đang hiệu lực
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.dim_users;

CREATE TABLE gold.dim_users (
    user_sk         BIGSERIAL   PRIMARY KEY,   -- surrogate key
    user_id         INTEGER     NOT NULL,       -- natural key
    full_name       TEXT,
    email           TEXT,
    phone           TEXT,
    city            TEXT,
    country         TEXT,
    is_active       BOOLEAN,

    -- SCD Type 2 tracking columns
    effective_from  DATE        NOT NULL,
    effective_to    DATE,                       -- NULL = còn hiệu lực
    is_current      BOOLEAN     NOT NULL DEFAULT TRUE,

    -- Audit
    _silver_batch   TEXT,
    _gold_loaded_at TIMESTAMP   DEFAULT NOW()
);

-- Load lần đầu: tất cả user từ Silver đều là "current"
-- effective_from = created_date của user (hoặc ngày load nếu NULL)
INSERT INTO gold.dim_users (
    user_id, full_name, email, phone,
    city, country, is_active,
    effective_from, effective_to, is_current,
    _silver_batch
)
SELECT
    user_id,
    full_name,
    email,
    phone,
    city,
    country,
    COALESCE(is_active, FALSE),
    COALESCE(created_date, CURRENT_DATE)   AS effective_from,
    NULL                                   AS effective_to,       -- còn hiệu lực
    TRUE                                   AS is_current,
    _source_batch_id
FROM silver.users;

CREATE INDEX idx_dim_users_user_id    ON gold.dim_users(user_id);
CREATE INDEX idx_dim_users_is_current ON gold.dim_users(is_current);

SELECT 'gold.dim_users' AS tbl, COUNT(*) AS rows FROM gold.dim_users;


-- ════════════════════════════════════════════════════════════
-- 2b. STORED PROCEDURE — SCD Type 2 UPSERT cho dim_users
--
-- Dùng khi chạy pipeline định kỳ (daily/weekly):
--   CALL gold.upsert_dim_users();
--
-- Logic:
--   - User chưa có → INSERT mới (is_current=TRUE)
--   - User đã có, có thay đổi → đóng row cũ, INSERT row mới
--   - User đã có, không thay đổi → bỏ qua
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE gold.upsert_dim_users()
LANGUAGE plpgsql
AS $$
DECLARE
    changed_count   INTEGER := 0;
    new_count       INTEGER := 0;
BEGIN

    -- B1: Đóng các record cũ nếu user có thay đổi ở Silver
    UPDATE gold.dim_users d
    SET
        effective_to = CURRENT_DATE - 1,
        is_current   = FALSE
    FROM silver.users s
    WHERE d.user_id    = s.user_id
      AND d.is_current = TRUE
      AND (
          d.full_name  IS DISTINCT FROM s.full_name  OR
          d.email      IS DISTINCT FROM s.email       OR
          d.phone      IS DISTINCT FROM s.phone       OR
          d.city       IS DISTINCT FROM s.city        OR
          d.country    IS DISTINCT FROM s.country     OR
          d.is_active  IS DISTINCT FROM s.is_active
      );

    GET DIAGNOSTICS changed_count = ROW_COUNT;

    -- B2: Insert version mới cho các user vừa bị đóng
    INSERT INTO gold.dim_users (
        user_id, full_name, email, phone,
        city, country, is_active,
        effective_from, effective_to, is_current,
        _silver_batch
    )
    SELECT
        s.user_id, s.full_name, s.email, s.phone,
        s.city, s.country, COALESCE(s.is_active, FALSE),
        CURRENT_DATE, NULL, TRUE,
        s._source_batch_id
    FROM silver.users s
    -- User đã có trong dim nhưng vừa bị đóng (không còn is_current)
    WHERE s.user_id IN (
        SELECT user_id FROM gold.dim_users WHERE is_current = FALSE AND effective_to = CURRENT_DATE - 1
    );

    -- B3: Insert user hoàn toàn mới (chưa từng có trong dim)
    INSERT INTO gold.dim_users (
        user_id, full_name, email, phone,
        city, country, is_active,
        effective_from, effective_to, is_current,
        _silver_batch
    )
    SELECT
        s.user_id, s.full_name, s.email, s.phone,
        s.city, s.country, COALESCE(s.is_active, FALSE),
        COALESCE(s.created_date, CURRENT_DATE), NULL, TRUE,
        s._source_batch_id
    FROM silver.users s
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_users d WHERE d.user_id = s.user_id
    );

    GET DIAGNOSTICS new_count = ROW_COUNT;

    RAISE NOTICE 'SCD2 upsert xong: % row đóng, % row mới', changed_count, new_count;
END;
$$;


-- ════════════════════════════════════════════════════════════
-- 3. DIM_PRODUCTS
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.dim_products;

CREATE TABLE gold.dim_products AS
SELECT
    product_id                          AS product_sk,   -- dùng luôn natural key làm SK (đơn giản)
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    CASE
        WHEN unit_price IS NOT NULL AND cost_price IS NOT NULL AND unit_price > 0
        THEN ROUND((unit_price - cost_price) / unit_price * 100, 2)
        ELSE NULL
    END                                 AS gross_margin_pct,
    stock_qty,
    supplier,
    NOW()                               AS _gold_loaded_at
FROM silver.products;

ALTER TABLE gold.dim_products ADD PRIMARY KEY (product_sk);
CREATE INDEX idx_dim_products_category ON gold.dim_products(category);

SELECT 'gold.dim_products' AS tbl, COUNT(*) AS rows FROM gold.dim_products;


-- ════════════════════════════════════════════════════════════
-- 4. FACT_ORDERS
--
-- Grain: 1 row = 1 order_item (bán hàng theo từng sản phẩm)
-- Measures: quantity, unit_price, subtotal, discount_amount
-- FK: dim_date, dim_users (SCD2 → lấy SK tại thời điểm order)
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.fact_orders;

CREATE TABLE gold.fact_orders AS
SELECT
    -- Surrogate keys → join với Dim
    oi.item_id                          AS order_item_sk,
    o.order_id,

    -- FK dim_date
    TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER
                                        AS order_date_key,

    -- FK dim_users: lấy user_sk hiệu lực tại thời điểm order (SCD2 lookup)
    du.user_sk,

    -- FK dim_products
    oi.product_id                       AS product_sk,

    -- Degenerate dimensions (thuộc tính của fact, không cần dim riêng)
    o.status                            AS order_status,
    o.shipping_city,

    -- Measures
    oi.quantity,
    oi.unit_price,
    oi.subtotal_calculated              AS gross_amount,
    ROUND(oi.subtotal_calculated * COALESCE(o.discount_pct, 0) / 100, 2)
                                        AS discount_amount,
    ROUND(oi.subtotal_calculated * (1 - COALESCE(o.discount_pct, 0) / 100), 2)
                                        AS net_amount,

    -- Audit
    NOW()                               AS _gold_loaded_at

FROM silver.order_items oi
JOIN silver.orders o
    ON oi.order_id = o.order_id
-- SCD2 lookup: tìm user_sk tại thời điểm order
LEFT JOIN gold.dim_users du
    ON du.user_id    = o.user_id
    AND du.is_current = TRUE            -- dùng current version (hoặc range nếu muốn exact SCD2)
WHERE o.order_date IS NOT NULL          -- bỏ row không có ngày
  AND o.total_amount IS NOT NULL;       -- bỏ row không có amount

CREATE INDEX idx_fact_orders_date_key   ON gold.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_user_sk    ON gold.fact_orders(user_sk);
CREATE INDEX idx_fact_orders_product_sk ON gold.fact_orders(product_sk);
CREATE INDEX idx_fact_orders_status     ON gold.fact_orders(order_status);

SELECT 'gold.fact_orders' AS tbl, COUNT(*) AS rows FROM gold.fact_orders;


-- ════════════════════════════════════════════════════════════
-- 5. FACT_TRANSACTIONS
--
-- Grain: 1 row = 1 transaction (payment event)
-- Measures: amount
-- ════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.fact_transactions;

CREATE TABLE gold.fact_transactions AS
SELECT
    t.txn_id                            AS txn_sk,
    t.order_id,

    TO_CHAR(t.txn_date, 'YYYYMMDD')::INTEGER
                                        AS txn_date_key,

    du.user_sk,

    -- Degenerate dimensions
    t.payment_method,
    t.status                            AS txn_status,
    t.currency,

    -- Measure
    t.amount,

    -- Derived: VND conversion (giả sử tỷ giá cố định cho demo)
    CASE t.currency
        WHEN 'USD' THEN ROUND(t.amount * 24500, 0)
        WHEN 'EUR' THEN ROUND(t.amount * 26800, 0)
        ELSE t.amount
    END                                 AS amount_vnd,

    NOW()                               AS _gold_loaded_at

FROM silver.transactions t
LEFT JOIN gold.dim_users du
    ON du.user_id    = t.user_id
    AND du.is_current = TRUE
WHERE t.txn_date IS NOT NULL
  AND t.amount IS NOT NULL;

CREATE INDEX idx_fact_txn_date_key ON gold.fact_transactions(txn_date_key);
CREATE INDEX idx_fact_txn_user_sk  ON gold.fact_transactions(user_sk);
CREATE INDEX idx_fact_txn_status   ON gold.fact_transactions(txn_status);

SELECT 'gold.fact_transactions' AS tbl, COUNT(*) AS rows FROM gold.fact_transactions;


-- ════════════════════════════════════════════════════════════
-- 6. BUSINESS VIEWS — sẵn sàng cho BI / Analytics
-- ════════════════════════════════════════════════════════════

-- 6a. Doanh thu theo tháng & danh mục sản phẩm
CREATE OR REPLACE VIEW gold.vw_revenue_by_month_category AS
SELECT
    dd.year,
    dd.quarter_name,
    dd.month_num,
    dd.month_name,
    dp.category,
    COUNT(DISTINCT fo.order_id)         AS total_orders,
    SUM(fo.quantity)                    AS total_qty,
    ROUND(SUM(fo.gross_amount), 0)      AS gross_revenue,
    ROUND(SUM(fo.discount_amount), 0)   AS total_discount,
    ROUND(SUM(fo.net_amount), 0)        AS net_revenue
FROM gold.fact_orders fo
JOIN gold.dim_date    dd ON fo.order_date_key = dd.date_key
JOIN gold.dim_products dp ON fo.product_sk    = dp.product_sk
WHERE fo.order_status NOT IN ('cancelled', 'returned')
GROUP BY dd.year, dd.quarter_name, dd.month_num, dd.month_name, dp.category
ORDER BY dd.year, dd.month_num, dp.category;


-- 6b. Phân tích người dùng theo thành phố
CREATE OR REPLACE VIEW gold.vw_user_orders_by_city AS
SELECT
    du.city,
    du.country,
    COUNT(DISTINCT fo.user_sk)          AS unique_buyers,
    COUNT(DISTINCT fo.order_id)         AS total_orders,
    ROUND(SUM(fo.net_amount), 0)        AS total_revenue,
    ROUND(AVG(fo.net_amount), 0)        AS avg_order_value
FROM gold.fact_orders fo
JOIN gold.dim_users   du ON fo.user_sk = du.user_sk
WHERE fo.order_status NOT IN ('cancelled', 'returned')
GROUP BY du.city, du.country
ORDER BY total_revenue DESC;


-- 6c. Payment method performance
CREATE OR REPLACE VIEW gold.vw_payment_summary AS
SELECT
    ft.payment_method,
    ft.currency,
    ft.txn_status,
    COUNT(*)                            AS txn_count,
    ROUND(SUM(ft.amount_vnd), 0)        AS total_amount_vnd,
    ROUND(AVG(ft.amount_vnd), 0)        AS avg_amount_vnd
FROM gold.fact_transactions ft
GROUP BY ft.payment_method, ft.currency, ft.txn_status
ORDER BY total_amount_vnd DESC;


-- 6d. Top 10 sản phẩm bán chạy
CREATE OR REPLACE VIEW gold.vw_top_products AS
SELECT
    dp.product_name,
    dp.category,
    dp.subcategory,
    SUM(fo.quantity)                    AS total_qty_sold,
    ROUND(SUM(fo.net_amount), 0)        AS total_net_revenue,
    ROUND(AVG(fo.unit_price), 0)        AS avg_selling_price,
    dp.gross_margin_pct
FROM gold.fact_orders fo
JOIN gold.dim_products dp ON fo.product_sk = dp.product_sk
WHERE fo.order_status = 'delivered'
GROUP BY dp.product_name, dp.category, dp.subcategory, dp.gross_margin_pct
ORDER BY total_net_revenue DESC
LIMIT 10;


-- ════════════════════════════════════════════════════════════
-- 7. FINAL SUMMARY
-- ════════════════════════════════════════════════════════════

SELECT 'gold.dim_date'          AS table_name, COUNT(*) AS row_count FROM gold.dim_date
UNION ALL
SELECT 'gold.dim_users',                        COUNT(*)              FROM gold.dim_users
UNION ALL
SELECT 'gold.dim_products',                     COUNT(*)              FROM gold.dim_products
UNION ALL
SELECT 'gold.fact_orders',                      COUNT(*)              FROM gold.fact_orders
UNION ALL
SELECT 'gold.fact_transactions',                COUNT(*)              FROM gold.fact_transactions;

-- Test một business view
SELECT * FROM gold.vw_revenue_by_month_category LIMIT 5;
SELECT * FROM gold.vw_top_products;