-- =============================================================
-- verify_bronze.sql
-- Chạy sau pipeline_bronze.py để xác nhận Bronze Layer OK
-- Kết nối: psql -U erp_user -d erp_source -f verify_bronze.sql
-- =============================================================


-- ── 1. Đếm row từng bảng Bronze ──────────────────────────────
SELECT 'bronze.users'        AS table_name, COUNT(*) AS total_rows FROM bronze.users
UNION ALL
SELECT 'bronze.products',                   COUNT(*)               FROM bronze.products
UNION ALL
SELECT 'bronze.orders',                     COUNT(*)               FROM bronze.orders
UNION ALL
SELECT 'bronze.order_items',                COUNT(*)               FROM bronze.order_items
UNION ALL
SELECT 'bronze.transactions',               COUNT(*)               FROM bronze.transactions
ORDER BY table_name;


-- ── 2. Xem batch nào đã chạy ─────────────────────────────────
SELECT
    _batch_id,
    _source_table,
    MIN(_ingested_at::timestamptz) AS batch_start,
    MAX(_ingested_at::timestamptz) AS batch_end,
    COUNT(*)                        AS row_count
FROM bronze.users
GROUP BY _batch_id, _source_table
ORDER BY batch_start DESC;


-- ── 3. Confirm data bẩn vẫn còn trong Bronze ─────────────────

-- 3a. Email sai format (không có @)
SELECT _batch_id, email
FROM bronze.users
WHERE email NOT LIKE '%@%'
LIMIT 10;

-- 3b. Ngày tháng sai format (không phải yyyy-mm-dd)
SELECT _batch_id, order_date
FROM bronze.orders
WHERE order_date !~ '^\d{4}-\d{2}-\d{2}$'
  AND order_date IS NOT NULL
LIMIT 10;

-- 3c. Giá trị âm trong transactions
SELECT _batch_id, amount
FROM bronze.transactions
WHERE amount::numeric < 0
LIMIT 10;

-- 3d. NULL ở cột phone của users
SELECT COUNT(*) AS null_phone_count
FROM bronze.users
WHERE phone IS NULL;

-- 3e. Duplicate: cùng email xuất hiện nhiều lần
SELECT email, COUNT(*) AS cnt
FROM bronze.users
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 10;


-- ── 4. Xem cấu trúc bảng Bronze (có metadata columns) ─────────
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze' AND table_name = 'users'
ORDER BY ordinal_position;