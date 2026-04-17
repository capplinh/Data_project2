# SO SÁNH: MySQL (Project 1) vs AWS Glue (Project 2)

## 🎯 Mục Tiêu Document

Chứng minh rằng: **Logic trong MySQL = Logic trong Glue**. Chỉ khác "vỏ bọc" (syntax).

---

## 📊 Kiến Trúc Tổng Quan

### Project 1: MySQL-based Pipeline
```
CRM/ERP (Source)
    ↓ JDBC Connection
Bronze (MySQL Tables)
    ↓ SQL Transformations
Silver (MySQL Tables)
    ↓ SQL Joins & Aggregations
Gold (MySQL Tables - Star Schema)
    ↓ SQL Queries
BI Tools (Power BI/Tableau)
```

### Project 2: AWS Glue-based Pipeline
```
CRM/ERP (Source)
    ↓ Glue Connection / S3 Upload
S3 Landing Zone
    ↓ Glue Job 1 (PySpark)
S3 Bronze (Parquet)
    ↓ Glue Job 2 (PySpark)
S3 Silver (Parquet)
    ↓ Glue Job 3 (PySpark)
S3 Gold (Iceberg Tables)
    ↓ Athena Queries
BI Tools (QuickSight)
```

**Kết luận:** Cấu trúc giống hệt nhau! Chỉ thay MySQL → S3, SQL → PySpark.

---

## 🔄 So Sánh Từng Layer

### BRONZE LAYER

#### MySQL (Project 1)
```sql
-- Tạo table
CREATE TABLE bronze.crm_cust_info (
    customer_id VARCHAR(50),
    customer_name VARCHAR(200),
    email VARCHAR(100),
    phone VARCHAR(50),
    registration_date VARCHAR(50),
    -- Metadata
    _source VARCHAR(50),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(100)
);

-- Load data
INSERT INTO bronze.crm_cust_info
SELECT 
    *,
    'CRM' as _source,
    NOW() as _load_timestamp,
    UUID() as _batch_id
FROM source_crm.customers;
```

#### AWS Glue (Project 2)
```python
# Glue Job 1: Landing → Bronze
from awsglue.context import GlueContext
from pyspark.sql.functions import lit, current_timestamp, expr

# Read from source
df_source = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "url": "jdbc:mysql://crm-db:3306/source_crm",
        "dbtable": "customers"
    }
).toDF()

# Add metadata
df_bronze = df_source \
    .withColumn("_source", lit("CRM")) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_batch_id", expr("uuid()"))

# Write to S3 Bronze
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df_bronze, glueContext, "df_bronze"),
    connection_type="s3",
    connection_options={
        "path": "s3://bucket/raw/crm_cust_info/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)
```

**Logic giống nhau:**
- ✅ Load dữ liệu từ source
- ✅ Thêm metadata columns (_source, _load_timestamp, _batch_id)
- ✅ Không transform business logic

---

### SILVER LAYER

#### MySQL (Project 1)
```sql
-- Clean và validate
INSERT INTO silver.clean_crm_cust_info
SELECT DISTINCT
    customer_id,
    TRIM(customer_name) as customer_name,
    CASE 
        WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email))
        ELSE NULL 
    END as email,
    REPLACE(REPLACE(REPLACE(phone, '-', ''), ' ', ''), '+84', '0') as phone,
    STR_TO_DATE(registration_date, '%Y-%m-%d') as registration_date,
    LOWER(TRIM(status)) as status,
    -- Quality score
    (CASE WHEN customer_id IS NOT NULL THEN 0.2 ELSE 0 END +
     CASE WHEN customer_name IS NOT NULL THEN 0.2 ELSE 0 END +
     CASE WHEN email IS NOT NULL THEN 0.2 ELSE 0 END +
     CASE WHEN phone IS NOT NULL THEN 0.2 ELSE 0 END +
     CASE WHEN registration_date IS NOT NULL THEN 0.2 ELSE 0 END
    ) as quality_score
FROM bronze.crm_cust_info
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL
  AND STR_TO_DATE(registration_date, '%Y-%m-%d') IS NOT NULL;
```

#### AWS Glue (Project 2)
```python
# Glue Job 2: Bronze → Silver
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_date, when
)

# Read Bronze
df_bronze = spark.read.parquet("s3://bucket/raw/crm_cust_info/")

# Clean và validate
df_silver = df_bronze \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("customer_name").isNotNull()) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("email", 
        when(col("email").rlike(".*@.*\\..*"), 
             lower(trim(col("email"))))
        .otherwise(None)
    ) \
    .withColumn("phone", 
        regexp_replace(
            regexp_replace(
                regexp_replace(col("phone"), "-", ""),
                " ", ""
            ),
            "\\+84", "0"
        )
    ) \
    .withColumn("registration_date", 
        to_date(col("registration_date"), "yyyy-MM-dd")
    ) \
    .withColumn("status", lower(trim(col("status")))) \
    .filter(col("registration_date").isNotNull())

# Calculate quality score
df_silver = df_silver.withColumn("quality_score",
    (when(col("customer_id").isNotNull(), 0.2).otherwise(0) +
     when(col("customer_name").isNotNull(), 0.2).otherwise(0) +
     when(col("email").isNotNull(), 0.2).otherwise(0) +
     when(col("phone").isNotNull(), 0.2).otherwise(0) +
     when(col("registration_date").isNotNull(), 0.2).otherwise(0))
)

# Write to S3 Silver
df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://bucket/processed/clean_crm_cust_info/")
```

**Logic giống nhau:**
- ✅ Filter NULL values
- ✅ Remove duplicates
- ✅ Trim và lowercase strings
- ✅ Validate email format
- ✅ Standardize phone numbers
- ✅ Parse dates
- ✅ Calculate quality score

---

### GOLD LAYER - Dimension (SCD Type 2)

#### MySQL (Project 1)
```sql
-- Load dim_customers với SCD Type 2
MERGE INTO gold.dim_customers AS target
USING (
    SELECT 
        customer_id,
        customer_name,
        email,
        city,
        region
    FROM silver.clean_crm_cust_info
) AS source
ON target.customer_id = source.customer_id 
   AND target.is_current = 1

-- Khi có thay đổi
WHEN MATCHED AND (
    target.customer_name <> source.customer_name OR
    target.city <> source.city
) THEN
    UPDATE SET 
        is_current = 0,
        end_date = CURRENT_DATE

-- Insert new version
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, city, region,
            effective_date, end_date, is_current)
    VALUES (source.customer_id, source.customer_name, source.email,
            source.city, source.region,
            CURRENT_DATE, '9999-12-31', 1);

-- Insert new version cho records đã thay đổi
INSERT INTO gold.dim_customers (...)
SELECT ... FROM source
WHERE customer_id IN (
    SELECT customer_id FROM gold.dim_customers 
    WHERE is_current = 0 AND end_date = CURRENT_DATE
);
```

#### AWS Glue (Project 2) - Iceberg MERGE
```python
# Glue Job 3: Silver → Gold (Iceberg)
from pyspark.sql.functions import col, current_date, lit

# Read Silver
df_silver = spark.read.parquet("s3://bucket/processed/clean_crm_cust_info/")

# Read existing Gold dimension (Iceberg table)
df_gold = spark.read.format("iceberg") \
    .load("glue_catalog.gold.dim_customers")

# Identify changes
df_changes = df_silver.alias("s") \
    .join(
        df_gold.filter(col("is_current") == 1).alias("g"),
        col("s.customer_id") == col("g.customer_id"),
        "left"
    ) \
    .where(
        col("g.customer_id").isNull() |  # New customer
        (col("s.customer_name") != col("g.customer_name")) |  # Name changed
        (col("s.city") != col("g.city"))  # City changed
    )

# Close old versions
spark.sql(f"""
    MERGE INTO glue_catalog.gold.dim_customers AS target
    USING changes AS source
    ON target.customer_id = source.customer_id 
       AND target.is_current = 1
    WHEN MATCHED THEN
        UPDATE SET 
            is_current = 0,
            end_date = current_date()
""")

# Insert new versions
df_new_versions = df_changes.select(
    col("customer_id"),
    col("customer_name"),
    col("email"),
    col("city"),
    col("region"),
    current_date().alias("effective_date"),
    lit("9999-12-31").cast("date").alias("end_date"),
    lit(1).alias("is_current")
)

df_new_versions.write \
    .format("iceberg") \
    .mode("append") \
    .save("glue_catalog.gold.dim_customers")
```

**Logic giống nhau:**
- ✅ Detect changes (compare current vs new)
- ✅ Close old versions (set is_current = 0)
- ✅ Insert new versions
- ✅ Maintain history (SCD Type 2)

---

### GOLD LAYER - Fact Table

#### MySQL (Project 1)
```sql
-- Load fact_sales
INSERT INTO gold.fact_sales (
    date_key, customer_key, product_key,
    transaction_id, quantity, unit_price, total_amount
)
SELECT 
    dd.date_key,
    dc.customer_key,
    dp.product_key,
    s.transaction_id,
    s.quantity,
    s.unit_price,
    s.total_amount
FROM silver.clean_crm_sales_details s
INNER JOIN gold.dim_date dd 
    ON DATE(s.transaction_date) = dd.full_date
INNER JOIN gold.dim_customers dc 
    ON s.customer_id = dc.customer_id 
    AND dc.is_current = 1
INNER JOIN gold.dim_products dp 
    ON s.product_id = dp.product_id 
    AND dp.is_current = 1
WHERE NOT EXISTS (
    SELECT 1 FROM gold.fact_sales f
    WHERE f.transaction_id = s.transaction_id
);
```

#### AWS Glue (Project 2)
```python
# Glue Job 3: Load fact_sales
from pyspark.sql.functions import col, to_date

# Read Silver
df_sales = spark.read.parquet("s3://bucket/processed/clean_crm_sales_details/")

# Read dimensions
df_date = spark.read.format("iceberg").load("glue_catalog.gold.dim_date")
df_customers = spark.read.format("iceberg") \
    .load("glue_catalog.gold.dim_customers") \
    .filter(col("is_current") == 1)
df_products = spark.read.format("iceberg") \
    .load("glue_catalog.gold.dim_products") \
    .filter(col("is_current") == 1)

# Join với dimensions
df_fact = df_sales \
    .join(df_date, 
          to_date(col("transaction_date")) == col("full_date"),
          "inner") \
    .join(df_customers,
          col("customer_id") == df_customers["customer_id"],
          "inner") \
    .join(df_products,
          col("product_id") == df_products["product_id"],
          "inner") \
    .select(
        col("date_key"),
        col("customer_key"),
        col("product_key"),
        col("transaction_id"),
        col("quantity"),
        col("unit_price"),
        col("total_amount")
    )

# Read existing fact table
df_existing = spark.read.format("iceberg") \
    .load("glue_catalog.gold.fact_sales")

# Filter out duplicates
df_new = df_fact.join(
    df_existing.select("transaction_id"),
    "transaction_id",
    "left_anti"
)

# Write to Iceberg
df_new.write \
    .format("iceberg") \
    .mode("append") \
    .save("glue_catalog.gold.fact_sales")
```

**Logic giống nhau:**
- ✅ Join Silver với Dimensions
- ✅ Lookup dimension keys
- ✅ Filter current versions only
- ✅ Avoid duplicates
- ✅ Append to fact table

---

## 📊 ANALYTICAL QUERIES

### Query: Top Customers by Revenue

#### MySQL (Project 1)
```sql
SELECT 
    dc.customer_name,
    dc.city,
    SUM(fs.total_amount) as total_revenue,
    COUNT(DISTINCT fs.transaction_id) as total_transactions
FROM gold.fact_sales fs
INNER JOIN gold.dim_customers dc 
    ON fs.customer_key = dc.customer_key
WHERE dc.is_current = 1
GROUP BY dc.customer_name, dc.city
ORDER BY total_revenue DESC
LIMIT 10;
```

#### AWS Athena (Project 2)
```sql
SELECT 
    dc.customer_name,
    dc.city,
    SUM(fs.total_amount) as total_revenue,
    COUNT(DISTINCT fs.transaction_id) as total_transactions
FROM "glue_catalog"."gold"."fact_sales" fs
INNER JOIN "glue_catalog"."gold"."dim_customers" dc 
    ON fs.customer_key = dc.customer_key
WHERE dc.is_current = 1
GROUP BY dc.customer_name, dc.city
ORDER BY total_revenue DESC
LIMIT 10;
```

**Kết luận:** SQL giống hệt nhau! Chỉ khác tên catalog.

---

## 🔧 OPERATIONS

### Incremental Load

#### MySQL (Project 1)
```sql
-- Load chỉ dữ liệu mới
INSERT INTO bronze.crm_cust_info
SELECT *
FROM source_crm.customers
WHERE last_modified_date > (
    SELECT MAX(_load_timestamp) 
    FROM bronze.crm_cust_info
);
```

#### AWS Glue (Project 2)
```python
# Glue Job với bookmark
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Glue tự động track last processed timestamp
df_new = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "url": "jdbc:mysql://crm-db:3306/source_crm",
        "dbtable": "customers",
        "jobBookmarkKeys": ["last_modified_date"],
        "jobBookmarkKeysSortOrder": "asc"
    }
).toDF()

# Write to S3
df_new.write.mode("append").parquet("s3://bucket/raw/crm_cust_info/")

job.commit()  # Save bookmark
```

**Logic giống nhau:** Chỉ load dữ liệu mới dựa trên timestamp.

---

## 💰 CHI PHÍ & PERFORMANCE

| Tiêu chí | MySQL (Project 1) | AWS Glue (Project 2) |
|----------|-------------------|----------------------|
| **Setup** | Dễ, local | Phức tạp hơn, cloud |
| **Chi phí** | Thấp (server riêng) | Pay-as-you-go (DPU hours) |
| **Scalability** | Giới hạn (vertical) | Unlimited (horizontal) |
| **Performance** | Tốt cho < 1TB | Tốt cho > 1TB |
| **Maintenance** | Tự quản lý | AWS quản lý |
| **Learning curve** | Thấp (SQL quen thuộc) | Cao (PySpark + AWS) |

---

## 🎯 KẾT LUẬN

### Điểm Giống Nhau (95%)
1. ✅ **Logic ETL:** Extract → Transform → Load
2. ✅ **Data Quality:** Validate, clean, deduplicate
3. ✅ **Star Schema:** Dimensions + Facts
4. ✅ **SCD Type 2:** Track history
5. ✅ **Incremental Load:** Chỉ load dữ liệu mới
6. ✅ **Analytical Queries:** SQL giống hệt nhau

### Điểm Khác Nhau (5%)
1. ❌ **Storage:** MySQL tables vs S3 Parquet/Iceberg
2. ❌ **Compute:** MySQL engine vs Spark on Glue
3. ❌ **Syntax:** SQL vs PySpark
4. ❌ **Infrastructure:** On-prem vs Cloud

### Chiến Lược Học Tập

```
Bước 1: Master MySQL (Project 1)
    ↓ Hiểu rõ logic ETL
    ↓ Debug dễ dàng với SQL
    ↓ Chạy thử nhiều lần
    
Bước 2: Translate sang PySpark
    ↓ SQL → PySpark syntax
    ↓ Giữ nguyên logic
    
Bước 3: Deploy lên AWS Glue (Project 2)
    ↓ Chỉ thay đổi infrastructure
    ↓ Logic không đổi!
```

**Nhớ:** Bạn không học 2 thứ khác nhau. Bạn học 1 thứ (ETL logic) với 2 cách implement!

---

## 📚 Tài Liệu Liên Quan

- [MySQL Execution Guide](./MYSQL_EXECUTION_GUIDE.md)
- [Giải Thích 3 Layer](./GIAI_THICH_3_LAYER.md)
- [Quick Start](../QUICKSTART.md)
