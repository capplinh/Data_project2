# HƯỚNG DẪN CHẠY THỰC TÊ TRÊN MYSQL - HIỂU LUỒNG XỬ LÝ

## 🎯 Mục Tiêu

Chạy thử toàn bộ pipeline Bronze → Silver → Gold trên MySQL để:
1. **Hiểu rõ logic** từng bước transformation
2. **Debug dễ dàng** với SQL quen thuộc
3. **Chuẩn bị mindset** trước khi nhảy sang AWS Glue

> **Nguyên tắc:** Logic trong MySQL = Logic trong Glue. Chỉ khác "vỏ bọc" (MySQL vs PySpark).

---

## 📋 Chuẩn Bị

### 1. Tạo Database và Schemas
```sql
-- Tạo 3 schemas tương ứng 3 layers
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
```

### 2. Kiểm Tra Cấu Trúc Hiện Tại
```bash
# Xem cấu trúc project
ls -la bronze/ddl/
ls -la silver/ddl/
ls -la gold/ddl/
```

---

## 🚀 BƯỚC 1: BRONZE LAYER - Load Dữ Liệu Thô

### Mục Tiêu
- Load dữ liệu từ "source systems" (giả lập)
- Giữ nguyên format gốc
- Thêm metadata columns

### Thực Hiện

```bash
# 1. Tạo tables trong Bronze
mysql -u root -p < bronze/ddl/crm_tables.sql
mysql -u root -p < bronze/ddl/erp_tables.sql

# 2. Load sample data (giả lập data từ CRM/ERP)
mysql -u root -p < testing/load_sample_data.sql
```


### Kiểm Tra Dữ Liệu Bronze

```sql
-- Xem dữ liệu thô từ CRM
USE bronze;
SELECT * FROM crm_cust_info LIMIT 10;
SELECT * FROM crm_prd_info LIMIT 10;
SELECT * FROM crm_sales_details LIMIT 10;

-- Xem dữ liệu thô từ ERP
SELECT * FROM erp_cust_az12 LIMIT 10;
SELECT * FROM erp_loc_a101 LIMIT 10;
SELECT * FROM erp_px_cat_g1v2 LIMIT 10;

-- Đếm số records
SELECT 'crm_cust_info' as table_name, COUNT(*) as row_count FROM crm_cust_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details;
```

### Quan Sát Vấn Đề Trong Bronze

```sql
-- Tìm duplicates
SELECT customer_id, COUNT(*) as dup_count
FROM bronze.crm_cust_info
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Tìm NULL values
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_phones
FROM bronze.crm_cust_info;

-- Tìm invalid dates
SELECT customer_id, registration_date
FROM bronze.crm_cust_info
WHERE registration_date IS NULL 
   OR STR_TO_DATE(registration_date, '%Y-%m-%d') IS NULL;
```

**Kết luận:** Bronze có nhiều vấn đề → Cần làm sạch ở Silver!

---

## 🧹 BƯỚC 2: SILVER LAYER - Làm Sạch Dữ Liệu

### Mục Tiêu
- Loại bỏ duplicates
- Xử lý NULL values
- Chuẩn hóa format
- Validate business rules

### Thực Hiện

```bash
# 1. Tạo tables trong Silver
mysql -u root -p < silver/ddl/silver_tables.sql

# 2. Chạy transformations
mysql -u root -p < silver/transform/clean_crm_cust_info.sql
mysql -u root -p < silver/transform/clean_crm_prd_info.sql
mysql -u root -p < silver/transform/clean_crm_sales_details.sql
mysql -u root -p < silver/transform/clean_erp_cust_az12.sql
mysql -u root -p < silver/transform/clean_erp_loc_a101.sql
mysql -u root -p < silver/transform/clean_erp_px_cat_g1v2.sql

# 3. Hoặc chạy stored procedure (nếu có)
mysql -u root -p -e "CALL silver.sp_load_silver();"
```

### Kiểm Tra Dữ Liệu Silver

```sql
USE silver;

-- So sánh số lượng records Bronze vs Silver
SELECT 
    'Bronze' as layer, COUNT(*) as row_count 
FROM bronze.crm_cust_info
UNION ALL
SELECT 
    'Silver', COUNT(*) 
FROM silver.clean_crm_cust_info;

-- Kiểm tra không còn duplicates
SELECT customer_id, COUNT(*) as dup_count
FROM silver.clean_crm_cust_info
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- Kết quả: Empty set (không còn duplicate)

-- Kiểm tra data quality
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as email_completeness,
    SUM(CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as phone_completeness
FROM silver.clean_crm_cust_info;
```

### Debug: Xem Records Bị Loại Bỏ

```sql
-- Records có trong Bronze nhưng không có trong Silver
SELECT b.*
FROM bronze.crm_cust_info b
LEFT JOIN silver.clean_crm_cust_info s ON b.customer_id = s.customer_id
WHERE s.customer_id IS NULL;

-- Lý do: NULL customer_id, invalid date, empty name, etc.
```

---

## ⭐ BƯỚC 3: GOLD LAYER - Mô Hình Hóa Star Schema

### Mục Tiêu
- Tạo Dimension tables (SCD Type 2)
- Tạo Fact tables
- Merge data từ nhiều nguồn (CRM + ERP)

### Thực Hiện

```bash
# 1. Tạo dimension tables
mysql -u root -p < gold/ddl/dim_customers.sql
mysql -u root -p < gold/ddl/dim_products.sql
mysql -u root -p < gold/ddl/dim_date.sql

# 2. Tạo fact tables
mysql -u root -p < gold/ddl/fact_sales.sql

# 3. Load dimensions
mysql -u root -p < gold/transform/load_dim_customers.sql
mysql -u root -p < gold/transform/load_dim_products.sql
mysql -u root -p < gold/transform/load_dim_date.sql

# 4. Load facts
mysql -u root -p < gold/transform/load_fact_sales.sql

# 5. Hoặc chạy stored procedure
mysql -u root -p -e "CALL gold.sp_load_gold();"
```

### Kiểm Tra Star Schema

```sql
USE gold;

-- Xem cấu trúc Star Schema
SHOW TABLES;
-- dim_customers, dim_products, dim_date, fact_sales

-- Kiểm tra dimensions
SELECT COUNT(*) as total_customers FROM dim_customers;
SELECT COUNT(*) as total_products FROM dim_products;
SELECT COUNT(*) as total_dates FROM dim_date;

-- Kiểm tra fact table
SELECT COUNT(*) as total_sales FROM fact_sales;

-- Verify foreign keys
SELECT 
    COUNT(*) as total_sales,
    COUNT(DISTINCT customer_key) as unique_customers,
    COUNT(DISTINCT product_key) as unique_products,
    COUNT(DISTINCT date_key) as unique_dates
FROM fact_sales;
```

---

## 🔍 BƯỚC 4: PHÂN TÍCH DỮ LIỆU (Giống Athena Query)

### Query 1: Doanh Thu Theo Khách Hàng

```sql
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.city,
    dc.region,
    COUNT(DISTINCT fs.transaction_id) as total_transactions,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_transaction_value
FROM gold.fact_sales fs
INNER JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.is_current = 1
GROUP BY dc.customer_id, dc.customer_name, dc.city, dc.region
ORDER BY total_revenue DESC
LIMIT 10;
```

### Query 2: Doanh Thu Theo Tháng (Trend Analysis)

```sql
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fs.transaction_id) as total_transactions,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.total_amount) - LAG(SUM(fs.total_amount)) OVER (ORDER BY dd.year, dd.month) as revenue_growth
FROM gold.fact_sales fs
INNER JOIN gold.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;
```

### Query 3: Top Products Theo Category

```sql
SELECT 
    dp.category,
    dp.product_name,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue
FROM gold.fact_sales fs
INNER JOIN gold.dim_products dp ON fs.product_key = dp.product_key
WHERE dp.is_current = 1
GROUP BY dp.category, dp.product_name
ORDER BY dp.category, total_revenue DESC;
```

### Query 4: SCD Type 2 - Lịch Sử Thay Đổi Customer

```sql
-- Xem lịch sử thay đổi của 1 customer
SELECT 
    customer_key,
    customer_id,
    customer_name,
    city,
    region,
    effective_date,
    end_date,
    is_current
FROM gold.dim_customers
WHERE customer_id = 'C001'
ORDER BY effective_date;

-- Phân tích doanh thu theo location history
SELECT 
    dc.customer_id,
    dc.city,
    dc.effective_date,
    dc.end_date,
    SUM(fs.total_amount) as revenue_in_this_location
FROM gold.fact_sales fs
INNER JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
INNER JOIN gold.dim_date dd ON fs.date_key = dd.date_key
WHERE dc.customer_id = 'C001'
  AND dd.full_date BETWEEN dc.effective_date AND dc.end_date
GROUP BY dc.customer_id, dc.city, dc.effective_date, dc.end_date
ORDER BY dc.effective_date;
```

---

## 🎓 BƯỚC 5: HIỂU LOGIC = HIỂU GLUE

### So Sánh MySQL vs AWS Glue

| MySQL (Project 1) | AWS Glue (Project 2) | Logic Giống Nhau |
|-------------------|----------------------|------------------|
| `INSERT INTO silver.clean_crm_cust_info SELECT ... FROM bronze.crm_cust_info` | `glue_context.write_dynamic_frame(df, "s3://bucket/silver/")` | ✅ Đều là ETL: Extract → Transform → Load |
| `WHERE customer_id IS NOT NULL` | `df.filter(col("customer_id").isNotNull())` | ✅ Đều là filter NULL |
| `SELECT DISTINCT customer_id` | `df.dropDuplicates(["customer_id"])` | ✅ Đều là dedup |
| `STR_TO_DATE(date_str, '%Y-%m-%d')` | `to_date(col("date_str"), "yyyy-MM-dd")` | ✅ Đều là parse date |
| `LEFT JOIN dim_customers` | `df.join(dim_customers_df, "customer_id", "left")` | ✅ Đều là join |

### Ví Dụ Cụ Thể: Clean Customer Data

**MySQL:**
```sql
INSERT INTO silver.clean_crm_cust_info
SELECT DISTINCT
    customer_id,
    TRIM(customer_name) as customer_name,
    LOWER(TRIM(email)) as email,
    REPLACE(REPLACE(phone, '-', ''), ' ', '') as phone,
    STR_TO_DATE(registration_date, '%Y-%m-%d') as registration_date
FROM bronze.crm_cust_info
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL;
```

**AWS Glue (PySpark):**
```python
from pyspark.sql.functions import col, trim, lower, regexp_replace, to_date

df_bronze = glueContext.create_dynamic_frame.from_catalog(
    database="bronze", table_name="crm_cust_info"
).toDF()

df_silver = df_bronze \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("customer_name").isNotNull()) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("phone", regexp_replace(regexp_replace(col("phone"), "-", ""), " ", "")) \
    .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))

glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df_silver, glueContext, "df_silver"),
    connection_type="s3",
    connection_options={"path": "s3://bucket/silver/clean_crm_cust_info/"},
    format="parquet"
)
```

**Kết luận:** Logic giống hệt nhau! Chỉ khác syntax.

---

## 📊 BƯỚC 6: TESTING & VALIDATION

### Test Script

```bash
# Chạy toàn bộ test suite
cd testing
./run_all_tests.sh

# Hoặc test từng layer
mysql -u root -p < testing/test_bronze.sql
mysql -u root -p < testing/test_silver.sql
mysql -u root -p < testing/test_gold.sql
```

### Manual Validation Checklist

```sql
-- ✅ Bronze: Có dữ liệu?
SELECT COUNT(*) FROM bronze.crm_cust_info;  -- > 0

-- ✅ Silver: Không còn duplicates?
SELECT customer_id, COUNT(*) FROM silver.clean_crm_cust_info 
GROUP BY customer_id HAVING COUNT(*) > 1;  -- Empty

-- ✅ Gold: Foreign keys hợp lệ?
SELECT COUNT(*) FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;  -- 0

-- ✅ Data lineage: Có thể trace từ Gold về Bronze?
SELECT 
    fs.transaction_id,
    fs.sales_key as gold_key,
    s.transaction_id as silver_id,
    b.transaction_id as bronze_id
FROM gold.fact_sales fs
INNER JOIN silver.clean_crm_sales_details s ON fs.transaction_id = s.transaction_id
INNER JOIN bronze.crm_sales_details b ON s.transaction_id = b.transaction_id
LIMIT 5;
```

---

## 🚀 BƯỚC 7: CHẠY FULL PIPELINE

### Script Tự Động

```bash
#!/bin/bash
# File: run_full_pipeline.sh

echo "=== STARTING DATA PIPELINE ==="

echo "Step 1: Load Bronze..."
mysql -u root -p < bronze/procedures/sp_load_bronze.sql
mysql -u root -p -e "CALL bronze.sp_load_bronze('FULL');"

echo "Step 2: Clean to Silver..."
mysql -u root -p < silver/procedures/sp_load_silver.sql
mysql -u root -p -e "CALL silver.sp_load_silver();"

echo "Step 3: Model to Gold..."
mysql -u root -p < gold/procedures/sp_load_gold.sql
mysql -u root -p -e "CALL gold.sp_load_gold();"

echo "Step 4: Run Tests..."
mysql -u root -p < testing/test_queries.sql

echo "=== PIPELINE COMPLETED ==="
```

### Monitoring

```sql
-- Tạo bảng log để track execution
CREATE TABLE IF NOT EXISTS etl_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    layer VARCHAR(20),
    procedure_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    rows_processed INT,
    error_message TEXT
);

-- Xem execution history
SELECT * FROM etl_log ORDER BY start_time DESC LIMIT 10;
```

---

## 🎯 KẾT LUẬN

### Bạn Đã Học Được Gì?

1. ✅ **Bronze:** Load raw data, thêm metadata, không transform
2. ✅ **Silver:** Clean, validate, standardize, dedup
3. ✅ **Gold:** Star Schema, SCD Type 2, business-ready
4. ✅ **Logic ETL:** Filter, join, aggregate, window functions
5. ✅ **Data Quality:** Tracking, validation, testing

### Bước Tiếp Theo

Khi đã hiểu rõ logic trên MySQL:
1. **Đọc code AWS Glue** → Nhận ra logic giống hệt
2. **Viết PySpark** → Chỉ là translate SQL sang Python
3. **Deploy lên S3/Glue** → Chỉ là thay đổi infrastructure

**Nhớ:** Logic không thay đổi. Chỉ có công cụ thay đổi!

---

## 📚 Tài Liệu Liên Quan

- [Giải Thích 3 Layer](./GIAI_THICH_3_LAYER.md)
- [Data Transformation Flow](./data_transformation_flow.md)
- [Testing Guide](../TESTING_GUIDE.md)
