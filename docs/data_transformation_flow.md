# Luồng Biến Đổi Dữ Liệu - 3 Layer Data Pipeline

## Tổng Quan

Document này giải thích chi tiết luồng biến đổi dữ liệu từ nguồn (Source Systems) qua 3 layer: Bronze → Silver → Gold, theo kiến trúc Data Lakehouse.

---

## 🎯 Mục Tiêu Của Từng Layer

| Layer | Mục Đích | Data Quality | Người Dùng |
|-------|----------|--------------|------------|
| **Bronze** | Lưu trữ dữ liệu thô (Raw) | 0% | Data Engineers |
| **Silver** | Làm sạch & Chuẩn hóa | 80-90% | Data Engineers, Data Scientists |
| **Gold** | Mô hình hóa Business | 95-99% | Business Analysts, BI Users |

---

## 📊 LAYER 1: BRONZE LAYER (Raw Data Zone)

### Vị Trí Trong Kiến Trúc
```
DATA SOURCES → INGESTION LAYER → [BRONZE LAYER] → Storage Layer
```

### Mục Đích
- Lưu trữ dữ liệu thô từ source systems **không có bất kỳ transformation nào**
- Giữ nguyên schema và data types từ nguồn
- Đóng vai trò như "single source of truth" cho raw data
- Hỗ trợ data lineage và audit trail
- Cho phép reprocess data nếu cần

### Đặc Điểm Kỹ Thuật

#### 1. Data Ingestion Strategy
```
┌─────────────────┐
│  Source System  │
└────────┬────────┘
         │
         ├─── Full Load (Cuối tuần)
         │    • Truncate & Reload toàn bộ
         │    • Dùng cho master data
         │
         └─── Incremental Load (Hàng ngày/giờ)
              • Load chỉ dữ liệu mới/thay đổi
              • Dùng timestamp hoặc CDC
              • Dùng cho transactional data
```

#### 2. Schema Design
```sql
-- Ví dụ: Bronze CRM Customer Table
CREATE TABLE bronze.crm_cust_info (
    -- Giữ nguyên tất cả columns từ source
    customer_id VARCHAR(50),
    customer_name VARCHAR(200),
    email VARCHAR(100),
    phone VARCHAR(50),
    address TEXT,
    registration_date VARCHAR(50),  -- Chưa convert sang DATE
    status VARCHAR(20),
    
    -- Metadata columns
    _source_system VARCHAR(50),     -- 'CRM'
    _load_timestamp TIMESTAMP,      -- Thời điểm load
    _batch_id VARCHAR(100)          -- Batch ID để tracking
);
```

#### 3. Dữ Liệu Thực Tế (Ví Dụ)
```
customer_id | customer_name | email              | phone        | registration_date | status
------------|---------------|--------------------|--------------|--------------------|--------
C001        | Nguyen Van A  | nguyenvana@gm.com  | 0901234567   | 2024-01-15        | active
C002        | Tran Thi B    | NULL               | 84-90-555-1234| 15/01/2024       | ACTIVE
C002        | Tran Thi B    | tranthib@gmail.com | 0905551234   | 2024-01-15        | active
C003        |               | customer3@test.com | 123          | 2024-13-45        | inactive

❌ Vấn đề trong Bronze:
- Duplicate records (C002)
- NULL values (email của C002)
- Inconsistent formats (phone, date)
- Invalid data (date 2024-13-45)
- Empty values (customer_name của C003)
- Inconsistent status values (active vs ACTIVE)
```

### ETL Process: sp_load_bronze

```sql
CREATE PROCEDURE sp_load_bronze(
    @load_type VARCHAR(20) = 'INCREMENTAL'  -- 'FULL' hoặc 'INCREMENTAL'
)
AS
BEGIN
    -- 1. Validate connection
    -- 2. Extract từ source
    -- 3. Load vào Bronze (append-only)
    -- 4. Log execution
    
    IF @load_type = 'FULL'
        TRUNCATE TABLE bronze.crm_cust_info;
    
    INSERT INTO bronze.crm_cust_info
    SELECT 
        *,
        'CRM' as _source_system,
        GETDATE() as _load_timestamp,
        NEWID() as _batch_id
    FROM source_crm.customers
    WHERE (@load_type = 'FULL' 
           OR last_modified_date > (SELECT MAX(_load_timestamp) 
                                    FROM bronze.crm_cust_info));
END;
```

### Monitoring & Logging
- Track số lượng records loaded
- Monitor data volume và performance
- Alert khi load fails
- Data freshness tracking

---

## 🧹 LAYER 2: SILVER LAYER (Cleaned Data Zone)

### Vị Trí Trong Kiến Trúc
```
BRONZE LAYER → PROCESSING & TRANSFORMATION LAYER → [SILVER LAYER] → Storage Layer
```

### Mục Đích
- Làm sạch dữ liệu (Data Cleansing)
- Chuẩn hóa định dạng (Standardization)
- Validate dữ liệu (Data Validation)
- Loại bỏ duplicates
- Áp dụng business rules
- Tính toán data quality scores

### Quá Trình Transformation Chi Tiết

#### 1. Data Cleansing
```sql
-- Ví dụ: Clean CRM Customer Data
CREATE PROCEDURE silver.clean_crm_cust_info
AS
BEGIN
    INSERT INTO silver.clean_crm_cust_info
    SELECT DISTINCT  -- Loại bỏ duplicates
        customer_id,
        
        -- Chuẩn hóa tên: Trim spaces, Title case
        TRIM(UPPER(LEFT(customer_name, 1)) + LOWER(SUBSTRING(customer_name, 2, LEN(customer_name)))) 
            as customer_name,
        
        -- Validate và chuẩn hóa email
        CASE 
            WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email))
            ELSE NULL 
        END as email,
        
        -- Chuẩn hóa phone: Chỉ giữ số
        REPLACE(REPLACE(REPLACE(phone, '-', ''), ' ', ''), '+84', '0') as phone,
        
        address,
        
        -- Convert sang DATE format chuẩn
        TRY_CONVERT(DATE, registration_date) as registration_date,
        
        -- Chuẩn hóa status: lowercase
        LOWER(TRIM(status)) as status,
        
        -- Metadata
        _source_system,
        _load_timestamp,
        GETDATE() as _silver_processed_timestamp
        
    FROM bronze.crm_cust_info
    WHERE customer_id IS NOT NULL  -- Loại bỏ records không có ID
      AND customer_name IS NOT NULL AND customer_name <> ''  -- Loại bỏ empty names
      AND TRY_CONVERT(DATE, registration_date) IS NOT NULL;  -- Chỉ giữ valid dates
END;
```

#### 2. Dữ Liệu Sau Khi Clean (Ví Dụ)
```
BEFORE (Bronze):
customer_id | customer_name | email              | phone          | registration_date | status
------------|---------------|--------------------|-----------------|--------------------|--------
C001        | Nguyen Van A  | nguyenvana@gm.com  | 0901234567     | 2024-01-15        | active
C002        | Tran Thi B    | NULL               | 84-90-555-1234 | 15/01/2024        | ACTIVE
C002        | Tran Thi B    | tranthib@gmail.com | 0905551234     | 2024-01-15        | active
C003        |               | customer3@test.com | 123            | 2024-13-45        | inactive

AFTER (Silver):
customer_id | customer_name | email              | phone      | registration_date | status
------------|---------------|--------------------|-----------|--------------------|--------
C001        | Nguyen van a  | nguyenvana@gm.com  | 0901234567| 2024-01-15        | active
C002        | Tran thi b    | tranthib@gmail.com | 0905551234| 2024-01-15        | active

✅ Improvements:
- Duplicates removed (chỉ giữ 1 record C002 với email hợp lệ)
- NULL emails handled
- Phone numbers standardized
- Date formats converted
- Status values normalized
- Invalid records filtered out (C003)
```

#### 3. Data Quality Scoring
```sql
-- Tính toán quality score cho mỗi record
ALTER TABLE silver.clean_crm_cust_info 
ADD quality_score DECIMAL(3,2);

UPDATE silver.clean_crm_cust_info
SET quality_score = (
    CASE WHEN customer_id IS NOT NULL THEN 0.2 ELSE 0 END +
    CASE WHEN customer_name IS NOT NULL THEN 0.2 ELSE 0 END +
    CASE WHEN email IS NOT NULL AND email LIKE '%@%.%' THEN 0.2 ELSE 0 END +
    CASE WHEN phone IS NOT NULL AND LEN(phone) >= 10 THEN 0.2 ELSE 0 END +
    CASE WHEN registration_date IS NOT NULL THEN 0.2 ELSE 0 END
);

-- Records với quality_score < 0.6 có thể cần review
```

#### 4. Business Rules Validation
```sql
-- Ví dụ: Validate business rules
-- Rule 1: Customer phải có ít nhất email HOẶC phone
-- Rule 2: Registration date không được trong tương lai
-- Rule 3: Status phải là 'active' hoặc 'inactive'

DELETE FROM silver.clean_crm_cust_info
WHERE (email IS NULL AND phone IS NULL)
   OR registration_date > GETDATE()
   OR status NOT IN ('active', 'inactive');
```

### ETL Process: sp_load_silver

```sql
CREATE PROCEDURE sp_load_silver
AS
BEGIN
    -- Clean từng table từ Bronze
    EXEC silver.clean_crm_cust_info;
    EXEC silver.clean_crm_prd_info;
    EXEC silver.clean_crm_sales_details;
    EXEC silver.clean_erp_cust_az12;
    EXEC silver.clean_erp_loc_a101;
    EXEC silver.clean_erp_px_cat_g1v2;
    
    -- Calculate quality scores
    -- Log execution results
END;
```

### Data Quality Metrics
- **Completeness:** % records có đầy đủ required fields
- **Accuracy:** % records pass validation rules
- **Consistency:** % records có format chuẩn
- **Uniqueness:** % records không duplicate

---

## ⭐ LAYER 3: GOLD LAYER (Business Data Zone)

### Vị Trí Trong Kiến Trúc
```
SILVER LAYER → PROCESSING & TRANSFORMATION LAYER → [GOLD LAYER] → Analytical Layer
```

### Mục Đích
- Mô hình hóa dữ liệu theo **Star Schema**
- Tạo **Dimension Tables** (SCD Type 2)
- Tạo **Fact Tables** với measures
- Tối ưu cho Business Intelligence
- Áp dụng business logic phức tạp
- Aggregation và pre-calculation

### Star Schema Model

```
         ┌─────────────────┐
         │  dim_customers  │
         │  (SCD Type 2)   │
         └────────┬────────┘
                  │
  ┌──────────────┼──────────────┐
  │              │               │
  │              ▼               │
  │      ┌──────────────┐       │
  │      │  fact_sales  │◄──────┘
  │      │              │
  │      └──────┬───────┘
  │             │
  ▼             ▼
┌──────────┐ ┌──────────────┐
│dim_date  │ │dim_products  │
│          │ │(SCD Type 2)  │
└──────────┘ └──────────────┘
```

### Dimension Tables (Slowly Changing Dimensions Type 2)

#### 1. dim_customers (Merge CRM + ERP Data)
```sql
CREATE TABLE gold.dim_customers (
    -- Surrogate Key
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business Key
    customer_id VARCHAR(50) NOT NULL,
    
    -- Attributes
    customer_name VARCHAR(200),
    customer_type VARCHAR(50),  -- 'Individual', 'Corporate'
    email VARCHAR(100),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    registration_date DATE,
    status VARCHAR(20),
    
    -- SCD Type 2 Columns
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BIT NOT NULL DEFAULT 1,
    
    -- Metadata
    created_timestamp TIMESTAMP DEFAULT GETDATE(),
    updated_timestamp TIMESTAMP
);
```

#### 2. Ví Dụ SCD Type 2 Implementation
```
Scenario: Customer C001 thay đổi địa chỉ từ "Hanoi" sang "Ho Chi Minh"

BEFORE Change:
customer_key | customer_id | customer_name | city   | effective_date | end_date   | is_current
-------------|-------------|---------------|--------|----------------|------------|------------
1            | C001        | Nguyen Van A  | Hanoi  | 2024-01-15     | 9999-12-31 | 1

AFTER Change (SCD Type 2):
customer_key | customer_id | customer_name | city        | effective_date | end_date   | is_current
-------------|-------------|---------------|-------------|----------------|------------|------------
1            | C001        | Nguyen Van A  | Hanoi       | 2024-01-15     | 2024-03-20 | 0  ← Closed
2            | C001        | Nguyen Van A  | Ho Chi Minh | 2024-03-21     | 9999-12-31 | 1  ← New

✅ Benefits:
- Giữ được lịch sử thay đổi
- Có thể phân tích theo thời điểm
- Fact table vẫn reference đúng dimension tại thời điểm transaction
```

#### 3. Load dim_customers (Merge Logic)
```sql
CREATE PROCEDURE gold.load_dim_customers
AS
BEGIN
    -- Merge data từ CRM và ERP
    WITH merged_customers AS (
        SELECT 
            COALESCE(crm.customer_id, erp.customer_id) as customer_id,
            COALESCE(crm.customer_name, erp.customer_name) as customer_name,
            COALESCE(crm.email, erp.email) as email,
            COALESCE(crm.phone, erp.phone) as phone,
            COALESCE(crm.address, erp.address) as address,
            erp.city,  -- ERP có thông tin location chi tiết hơn
            erp.region,
            erp.country,
            crm.registration_date,
            crm.status
        FROM silver.clean_crm_cust_info crm
        FULL OUTER JOIN silver.clean_erp_cust_az12 erp
            ON crm.customer_id = erp.customer_id
    )
    
    -- SCD Type 2 Logic
    MERGE gold.dim_customers AS target
    USING merged_customers AS source
    ON target.customer_id = source.customer_id 
       AND target.is_current = 1
    
    -- Khi có thay đổi attributes
    WHEN MATCHED AND (
        target.customer_name <> source.customer_name OR
        target.email <> source.email OR
        target.city <> source.city
    ) THEN
        UPDATE SET 
            is_current = 0,
            end_date = GETDATE()
    
    -- Insert new record cho thay đổi
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (customer_id, customer_name, email, phone, address, 
                city, region, country, registration_date, status,
                effective_date, end_date, is_current)
        VALUES (source.customer_id, source.customer_name, source.email, 
                source.phone, source.address, source.city, source.region, 
                source.country, source.registration_date, source.status,
                GETDATE(), '9999-12-31', 1);
    
    -- Insert new version cho records đã thay đổi
    INSERT INTO gold.dim_customers (
        customer_id, customer_name, email, phone, address,
        city, region, country, registration_date, status,
        effective_date, end_date, is_current
    )
    SELECT 
        customer_id, customer_name, email, phone, address,
        city, region, country, registration_date, status,
        GETDATE(), '9999-12-31', 1
    FROM merged_customers
    WHERE customer_id IN (
        SELECT customer_id 
        FROM gold.dim_customers 
        WHERE is_current = 0 
          AND end_date = CAST(GETDATE() AS DATE)
    );
END;
```

### Fact Tables

#### 1. fact_sales (Grain: Mỗi dòng = 1 sales transaction)
```sql
CREATE TABLE gold.fact_sales (
    -- Surrogate Key
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys (Dimension References)
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    
    -- Degenerate Dimensions (Transaction-level attributes)
    transaction_id VARCHAR(100) NOT NULL,
    order_number VARCHAR(100),
    
    -- Measures (Numeric facts)
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) DEFAULT 0,
    tax_amount DECIMAL(18,2) DEFAULT 0,
    total_amount DECIMAL(18,2) NOT NULL,
    
    -- Calculated Measures
    net_amount AS (total_amount - discount_amount - tax_amount),
    
    -- Metadata
    created_timestamp TIMESTAMP DEFAULT GETDATE(),
    
    -- Foreign Key Constraints
    FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key)
);
```

#### 2. Load fact_sales
```sql
CREATE PROCEDURE gold.load_fact_sales
AS
BEGIN
    INSERT INTO gold.fact_sales (
        date_key, customer_key, product_key,
        transaction_id, order_number,
        quantity, unit_price, discount_amount, tax_amount, total_amount
    )
    SELECT 
        -- Lookup dimension keys
        dd.date_key,
        dc.customer_key,
        dp.product_key,
        
        -- Transaction details
        s.transaction_id,
        s.order_number,
        s.quantity,
        s.unit_price,
        s.discount_amount,
        s.tax_amount,
        s.total_amount
        
    FROM silver.clean_crm_sales_details s
    
    -- Join với dim_date
    INNER JOIN gold.dim_date dd
        ON CAST(s.transaction_date AS DATE) = dd.full_date
    
    -- Join với dim_customers (chỉ lấy current version)
    INNER JOIN gold.dim_customers dc
        ON s.customer_id = dc.customer_id
        AND dc.is_current = 1
    
    -- Join với dim_products (chỉ lấy current version)
    INNER JOIN gold.dim_products dp
        ON s.product_id = dp.product_id
        AND dp.is_current = 1
    
    -- Chỉ load transactions chưa có trong fact table
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.fact_sales f
        WHERE f.transaction_id = s.transaction_id
    );
END;
```

### ETL Process: sp_load_gold

```sql
CREATE PROCEDURE sp_load_gold
AS
BEGIN
    -- Load dimensions first (vì fact table cần reference)
    EXEC gold.load_dim_date;
    EXEC gold.load_dim_customers;
    EXEC gold.load_dim_products;
    
    -- Load facts
    EXEC gold.load_fact_sales;
    
    -- Log execution
END;
```

### Business Intelligence Queries

#### Query 1: Total Sales by Customer
```sql
SELECT 
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
GROUP BY dc.customer_name, dc.city, dc.region
ORDER BY total_revenue DESC;
```

#### Query 2: Sales Trend by Month
```sql
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fs.transaction_id) as total_transactions,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.total_amount) - LAG(SUM(fs.total_amount)) OVER (ORDER BY dd.year, dd.month) 
        as revenue_growth
FROM gold.fact_sales fs
INNER JOIN gold.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;
```

---

## 🔄 Luồng Xử Lý End-to-End

### Timeline: Một Transaction Từ Source → Gold

```
T0: 10:00 AM - Transaction xảy ra tại CRM System
    └─ Customer C001 mua Product P100, Amount: 1,000,000 VND

T1: 11:00 AM - Batch Ingestion (Bronze)
    └─ sp_load_bronze chạy
    └─ Data được extract từ CRM và load vào bronze.crm_sales_details
    └─ Status: Raw data, chưa validate

T2: 12:00 PM - Data Cleansing (Silver)
    └─ sp_load_silver chạy
    └─ Validate: customer_id exists, product_id exists, amount > 0
    └─ Standardize: date format, amount format
    └─ Data được load vào silver.clean_crm_sales_details
    └─ Status: Cleaned & validated

T3: 01:00 PM - Data Modeling (Gold)
    └─ sp_load_gold chạy
    └─ Lookup customer_key từ dim_customers
    └─ Lookup product_key từ dim_products
    └─ Lookup date_key từ dim_date
    └─ Insert vào fact_sales
    └─ Status: Business-ready

T4: 01:30 PM - Business Intelligence
    └─ Power BI dashboard refresh
    └─ Transaction xuất hiện trong reports
    └─ Analysts có thể query và analyze
```

### Data Lineage
```
Source: CRM.sales_transactions (transaction_id: TXN001)
    ↓
Bronze: bronze.crm_sales_details (transaction_id: TXN001, _batch_id: B123)
    ↓
Silver: silver.clean_crm_sales_details (transaction_id: TXN001, quality_score: 0.95)
    ↓
Gold: gold.fact_sales (sales_key: 12345, transaction_id: TXN001)
    ↓
BI: Power BI Dashboard (Sales Report)
```

---

## 📈 Data Quality Evolution

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA QUALITY JOURNEY                      │
└─────────────────────────────────────────────────────────────┘

BRONZE (0% Quality)
├─ Raw data from sources
├─ Duplicates: ✗ Present
├─ NULL values: ✗ Present
├─ Invalid formats: ✗ Present
├─ Inconsistent data: ✗ Present
└─ Business rules: ✗ Not applied

        │ TRANSFORMATION
        ▼

SILVER (80-90% Quality)
├─ Cleaned & validated data
├─ Duplicates: ✓ Removed
├─ NULL values: ✓ Handled
├─ Invalid formats: ✓ Standardized
├─ Inconsistent data: ✓ Normalized
└─ Business rules: ✓ Partially applied

        │ MODELING
        ▼

GOLD (95-99% Quality)
├─ Business-ready data
├─ Duplicates: ✓ Removed
├─ NULL values: ✓ Handled
├─ Invalid formats: ✓ Standardized
├─ Inconsistent data: ✓ Normalized
├─ Business rules: ✓ Fully applied
├─ Star Schema: ✓ Modeled
└─ Optimized for BI: ✓ Yes
```

---

## 🎯 Best Practices

### Bronze Layer
- ✅ Giữ nguyên dữ liệu từ source (immutable)
- ✅ Append-only, không update/delete
- ✅ Lưu metadata (_source_system, _load_timestamp, _batch_id)
- ✅ Partition by date để optimize performance

### Silver Layer
- ✅ Idempotent transformations (có thể chạy lại nhiều lần)
- ✅ Document tất cả business rules
- ✅ Calculate và track data quality scores
- ✅ Log records bị reject để review

### Gold Layer
- ✅ Implement SCD Type 2 cho dimensions
- ✅ Use surrogate keys (không dùng business keys làm PK)
- ✅ Create indexes trên foreign keys
- ✅ Partition fact tables by date
- ✅ Pre-calculate common aggregations

---

## 🔍 Monitoring & Observability

### Key Metrics
- **Data Freshness:** Thời gian từ source → gold
- **Data Quality Score:** % records pass validation
- **ETL Success Rate:** % successful runs
- **Data Volume:** Records processed per layer
- **Processing Time:** Duration của mỗi ETL job

### Alerts
- ETL job failures
- Data quality score < threshold
- Data volume anomalies
- Processing time > SLA

---

## 📚 Tài Liệu Tham Khảo

- [Data Flow - Bronze Layer](./data_flow_bronze.md)
- [Data Flow - Silver Layer](./data_flow_silver.md)
- [Data Flow - Gold Layer](./data_flow_gold.md)
- [Data Catalog](./data_catalog.md)
- [Architecture Diagram](./architecture_diagram.md)
