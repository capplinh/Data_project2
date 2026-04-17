# Hướng Dẫn Test Database - MySQL

## Yêu Cầu

- MySQL 8.0+
- MySQL Workbench hoặc mysql client
- Quyền tạo database và tables

## Bước 1: Cài Đặt MySQL (nếu chưa có)

### macOS
```bash
brew install mysql
brew services start mysql
```

### Kiểm tra MySQL đang chạy
```bash
mysql --version
brew services list | grep mysql
```

## Bước 2: Tạo Database

```bash
# Kết nối MySQL (không cần password nếu mới cài)
mysql -u root

# Trong MySQL prompt, gõ:
CREATE DATABASE data_lakehouse;
USE data_lakehouse;
exit;
```

**LƯU Ý:** Nếu MySQL yêu cầu password mà bạn không nhớ, xem phần Troubleshooting ở cuối.

## Bước 3: Deploy Database Tables

**QUAN TRỌNG:** Không dùng file `deploy_all.sql` vì nó dùng PostgreSQL syntax. Chạy từng file như sau:

```bash
# Bronze Layer
mysql -u root data_lakehouse < bronze/ddl/crm_tables.sql
mysql -u root data_lakehouse < bronze/ddl/erp_tables.sql

# Silver Layer
mysql -u root data_lakehouse < silver/ddl/silver_tables.sql

# Gold Layer
mysql -u root data_lakehouse < gold/ddl/dim_customers.sql
mysql -u root data_lakehouse < gold/ddl/dim_products.sql
mysql -u root data_lakehouse < gold/ddl/dim_date.sql
mysql -u root data_lakehouse < gold/ddl/fact_sales.sql
```

### Kiểm tra tables đã tạo

```bash
mysql -u root data_lakehouse -e "SHOW TABLES;"
```

## Bước 4: Load Sample Data

```bash
# Load dữ liệu mẫu vào Bronze Layer
mysql -u root data_lakehouse < testing/load_sample_data.sql
```

## Bước 5: Test Bronze Layer

```bash
# Chạy test Bronze
mysql -u root data_lakehouse < testing/test_bronze.sql
```

Hoặc test trực tiếp:
```bash
mysql -u root data_lakehouse
```

```sql
-- Trong MySQL prompt:
-- Kiểm tra số lượng records
SELECT 'crm_cust_info' as table_name, COUNT(*) as row_count FROM crm_cust_info
UNION ALL SELECT 'crm_prd_info', COUNT(*) FROM crm_prd_info
UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
UNION ALL SELECT 'erp_cust_az12', COUNT(*) FROM erp_cust_az12
UNION ALL SELECT 'erp_loc_a101', COUNT(*) FROM erp_loc_a101
UNION ALL SELECT 'erp_px_cat_g1v2', COUNT(*) FROM erp_px_cat_g1v2;

-- Xem sample data
SELECT * FROM crm_cust_info LIMIT 5;
SELECT * FROM crm_sales_details LIMIT 5;
```

## Bước 6: Transform Silver Layer

```bash
# Chạy cleaning scripts
mysql -u root data_lakehouse < silver/transform/clean_crm_cust_info.sql
mysql -u root data_lakehouse < silver/transform/clean_crm_prd_info.sql
mysql -u root data_lakehouse < silver/transform/clean_crm_sales_details.sql
mysql -u root data_lakehouse < silver/transform/clean_erp_cust_az12.sql
mysql -u root data_lakehouse < silver/transform/clean_erp_loc_a101.sql
mysql -u root data_lakehouse < silver/transform/clean_erp_px_cat_g1v2.sql
```

## Bước 7: Test Silver Layer

```sql
-- Kiểm tra data quality
SELECT 'clean_crm_cust_info' as table_name, 
       COUNT(*) as row_count,
       ROUND(AVG(data_quality_score), 2) as avg_quality
FROM clean_crm_cust_info
UNION ALL
SELECT 'clean_crm_prd_info', COUNT(*), ROUND(AVG(data_quality_score), 2)
FROM clean_crm_prd_info;

-- Xem quality distribution
SELECT 
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent'
        WHEN data_quality_score >= 0.7 THEN 'Good'
        WHEN data_quality_score >= 0.5 THEN 'Fair'
        ELSE 'Poor'
    END as quality_level,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM clean_crm_cust_info), 2) as percentage
FROM clean_crm_cust_info
GROUP BY quality_level;
```

## Bước 8: Load Gold Layer

```bash
# Load date dimension trước tiên
mysql -u root data_lakehouse < gold/transform/load_dim_date.sql

# Load customer và product dimensions
mysql -u root data_lakehouse < gold/transform/load_dim_customers.sql
mysql -u root data_lakehouse < gold/transform/load_dim_products.sql

# Load fact table cuối cùng
mysql -u root data_lakehouse < gold/transform/load_fact_sales.sql
```

## Bước 9: Test Gold Layer

```sql
-- Kiểm tra dimensions
SELECT 'dim_customers' as dimension, 
       COUNT(*) as total_records,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*), SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM dim_products
UNION ALL
SELECT 'dim_date', COUNT(*), COUNT(*)
FROM dim_date;

-- Kiểm tra fact table
SELECT 
    COUNT(*) as total_sales,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction
FROM fact_sales;
```

## Bước 10: Test Star Schema Queries

```bash
# Chạy tất cả test queries
mysql -u root data_lakehouse < testing/test_queries.sql
```

Hoặc chạy từng query:

```sql
-- Top 5 Customers
SELECT 
    c.customer_name,
    c.city,
    COUNT(f.sales_key) as total_transactions,
    ROUND(SUM(f.total_amount), 2) as total_revenue
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_name, c.city
ORDER BY total_revenue DESC
LIMIT 5;

-- Sales by Product Category
SELECT 
    p.category,
    p.brand,
    COUNT(f.sales_key) as total_sales,
    SUM(f.quantity) as total_quantity,
    ROUND(SUM(f.total_amount), 2) as total_revenue
FROM fact_sales f
JOIN dim_products p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category, p.brand
ORDER BY total_revenue DESC;

-- Sales by Month
SELECT 
    d.year,
    d.month_name,
    COUNT(f.sales_key) as transactions,
    ROUND(SUM(f.total_amount), 2) as revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

## Bước 11: Test SCD Type 2

```sql
-- Xem customer hiện tại
SELECT customer_id, customer_name, email, 
       effective_date, end_date, is_current
FROM dim_customers
WHERE customer_id = 'CUST001';

-- Update customer trong Silver
UPDATE clean_crm_cust_info
SET email = 'newemail@example.com'
WHERE cust_id = 'CUST001';

-- Reload dimension
SOURCE gold/transform/load_dim_customers.sql;

-- Verify có 2 records (cũ và mới)
SELECT customer_id, email, effective_date, end_date, is_current
FROM dim_customers
WHERE customer_id = 'CUST001'
ORDER BY effective_date;
```

## Bước 12: Check ETL Logs

```sql
SELECT 
    procedure_name,
    layer,
    status,
    start_time,
    end_time,
    TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
    rows_processed
FROM etl_log
ORDER BY start_time DESC;
```

## QUICK START - Chạy Tất Cả Một Lần (KHUYẾN NGHỊ)

Tạo và chạy script tự động:

```bash
# Tạo script
cat > run_quick_test.sh << 'EOF'
#!/bin/bash
DB="data_lakehouse"

echo "=== Creating Tables ==="
mysql -u root $DB < bronze/ddl/crm_tables.sql
mysql -u root $DB < bronze/ddl/erp_tables.sql
mysql -u root $DB < silver/ddl/silver_tables.sql
mysql -u root $DB < gold/ddl/dim_customers.sql
mysql -u root $DB < gold/ddl/dim_products.sql
mysql -u root $DB < gold/ddl/dim_date.sql
mysql -u root $DB < gold/ddl/fact_sales.sql

echo "=== Loading Sample Data ==="
mysql -u root $DB < testing/load_sample_data.sql

echo "=== Transforming Silver ==="
mysql -u root $DB < silver/transform/clean_crm_cust_info.sql
mysql -u root $DB < silver/transform/clean_crm_prd_info.sql
mysql -u root $DB < silver/transform/clean_crm_sales_details.sql
mysql -u root $DB < silver/transform/clean_erp_cust_az12.sql
mysql -u root $DB < silver/transform/clean_erp_loc_a101.sql
mysql -u root $DB < silver/transform/clean_erp_px_cat_g1v2.sql

echo "=== Loading Gold ==="
mysql -u root $DB < gold/transform/load_dim_date.sql
mysql -u root $DB < gold/transform/load_dim_customers.sql
mysql -u root $DB < gold/transform/load_dim_products.sql
mysql -u root $DB < gold/transform/load_fact_sales.sql

echo "=== Running Tests ==="
mysql -u root $DB < testing/test_bronze.sql
mysql -u root $DB < testing/test_silver.sql
mysql -u root $DB < testing/test_gold.sql
mysql -u root $DB < testing/test_queries.sql

echo ""
echo "✓ HOÀN THÀNH! Tất cả tests đã chạy thành công!"
EOF

# Cho phép chạy
chmod +x run_quick_test.sh

# Chạy
./run_quick_test.sh
```

## Troubleshooting

### Lỗi: Access denied for user 'root'

Nếu MySQL yêu cầu password:

```bash
# Cách 1: Thử đăng nhập không password
mysql -u root

# Cách 2: Nếu có password, thêm -p
mysql -u root -p
# Nhập password khi được hỏi

# Cách 3: Reset password
brew services stop mysql
sudo rm -f /tmp/mysql*.sock*
brew services start mysql
sleep 5
mysql -u root
```

### Lỗi: MySQL không start được

```bash
# Xem log lỗi
tail -50 /opt/homebrew/var/mysql/*.err

# Xóa socket files
sudo rm -f /tmp/mysql*.sock*

# Restart
brew services restart mysql
```

### Lỗi: Port 3306 đã được sử dụng

```bash
# Tìm process đang dùng port
lsof -i :3306

# Kill process (thay PID)
sudo kill -9 [PID]

# Restart MySQL
brew services restart mysql
```

### Reset Database Hoàn Toàn

```bash
# Xóa database
mysql -u root -e "DROP DATABASE IF EXISTS data_lakehouse; CREATE DATABASE data_lakehouse;"

# Hoặc reinstall MySQL
brew services stop mysql
brew uninstall mysql
rm -rf /opt/homebrew/var/mysql
brew install mysql
brew services start mysql
```

## Kiểm Tra Kết Quả Cuối Cùng

```bash
# Xem tất cả tables
mysql -u root data_lakehouse -e "SHOW TABLES;"

# Đếm records trong mỗi layer
mysql -u root data_lakehouse -e "
SELECT 'crm_cust_info' as table_name, COUNT(*) as records FROM crm_cust_info
UNION ALL SELECT 'crm_prd_info', COUNT(*) FROM crm_prd_info
UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
UNION ALL SELECT 'clean_crm_cust_info', COUNT(*) FROM clean_crm_cust_info
UNION ALL SELECT 'dim_customers', COUNT(*) FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"

# Xem top 5 customers
mysql -u root data_lakehouse -e "
SELECT c.customer_name, c.city, COUNT(f.sales_key) as purchases, 
       ROUND(SUM(f.total_amount), 2) as total_spent
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_name, c.city
ORDER BY total_spent DESC
LIMIT 5;
"
```

## Các Lệnh Hữu Ích

```bash
# Đăng nhập MySQL
mysql -u root data_lakehouse

# Chạy query từ command line
mysql -u root data_lakehouse -e "SELECT COUNT(*) FROM fact_sales;"

# Export kết quả ra file
mysql -u root data_lakehouse -e "SELECT * FROM dim_customers;" > customers.txt

# Backup database
mysqldump -u root data_lakehouse > backup.sql

# Restore database
mysql -u root data_lakehouse < backup.sql
```
