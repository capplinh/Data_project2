# GIẢI THÍCH KIẾN TRÚC 3 LAYER - DATA PIPELINE

## 🎯 Tại Sao Cần 3 Layer?

Hãy tưởng tượng bạn đang xây một nhà:
- **Bronze** = Nguyên liệu thô (gạch, xi măng, sắt thép)
- **Silver** = Nguyên liệu đã xử lý (gạch đã chọn lọc, xi măng đã trộn)
- **Gold** = Ngôi nhà hoàn thiện (sẵn sàng để ở)

Tương tự, trong Data Pipeline:
- **Bronze** = Dữ liệu thô từ hệ thống nguồn
- **Silver** = Dữ liệu đã làm sạch và chuẩn hóa
- **Gold** = Dữ liệu đã mô hình hóa, sẵn sàng cho phân tích

---

## 📊 LAYER 1: BRONZE - Kho Dữ Liệu Thô

### Ví Dụ Thực Tế

Bạn có 2 hệ thống:
- **CRM** (quản lý khách hàng): MySQL database
- **ERP** (quản lý doanh nghiệp): Oracle database

Mỗi ngày, dữ liệu từ 2 hệ thống này được "chụp ảnh" và lưu vào Bronze Layer.

### Dữ Liệu Trông Như Thế Nào?

```
Bảng: bronze.crm_cust_info (Dữ liệu khách hàng từ CRM)

ID    | Tên           | Email              | Số ĐT         | Ngày ĐK    | Trạng thái
------|---------------|--------------------|--------------|-----------|-----------
C001  | Nguyen Van A  | nguyenvana@gm.com  | 0901234567   | 2024-01-15| active
C002  | Tran Thi B    | NULL               | 84-90-555-1234| 15/01/2024| ACTIVE
C002  | Tran Thi B    | tranthib@gmail.com | 0905551234   | 2024-01-15| active
C003  |               | customer3@test.com | 123          | 2024-13-45| inactive
```

### Vấn Đề Gì Ở Đây?

❌ **Duplicate:** C002 xuất hiện 2 lần
❌ **NULL:** Email của C002 (dòng 2) bị NULL
❌ **Format khác nhau:** 
   - Số điện thoại: "0901234567" vs "84-90-555-1234"
   - Ngày tháng: "2024-01-15" vs "15/01/2024"
   - Trạng thái: "active" vs "ACTIVE"
❌ **Dữ liệu không hợp lệ:** 
   - Tên khách hàng C003 bị trống
   - Ngày "2024-13-45" không tồn tại
   - Số điện thoại "123" quá ngắn

### Tại Sao Không Sửa Ngay?

✅ **Lý do giữ nguyên dữ liệu thô:**
1. **Audit Trail:** Có thể kiểm tra lại dữ liệu gốc nếu cần
2. **Reprocess:** Nếu logic xử lý thay đổi, có thể chạy lại từ đầu
3. **Compliance:** Một số quy định yêu cầu lưu dữ liệu gốc
4. **Debug:** Dễ dàng tìm lỗi khi biết dữ liệu gốc như thế nào

---

## 🧹 LAYER 2: SILVER - Kho Dữ Liệu Sạch

### Quá Trình Làm Sạch

Bronze Layer → **Xử lý** → Silver Layer

**Các bước xử lý:**

#### 1. Loại Bỏ Duplicates
```
C002 xuất hiện 2 lần → Chỉ giữ 1 record (record có email hợp lệ)
```

#### 2. Xử Lý NULL Values
```
Email = NULL → Kiểm tra xem có thể lấy từ nguồn khác không?
Nếu không → Để NULL hoặc loại bỏ record
```

#### 3. Chuẩn Hóa Format
```
Số điện thoại:
  "84-90-555-1234" → "0905551234" (bỏ dấu gạch, đổi +84 thành 0)
  
Ngày tháng:
  "15/01/2024" → "2024-01-15" (format chuẩn: YYYY-MM-DD)
  
Trạng thái:
  "ACTIVE" → "active" (lowercase)
```

#### 4. Validate Dữ Liệu
```
✓ Email phải có dạng: xxx@domain.com
✓ Số điện thoại phải có 10-15 chữ số
✓ Ngày tháng phải hợp lệ
✓ Tên khách hàng không được trống
```

#### 5. Loại Bỏ Records Không Hợp Lệ
```
C003: Tên trống, ngày không hợp lệ, số điện thoại quá ngắn → Loại bỏ
```

### Dữ Liệu Sau Khi Làm Sạch

```
Bảng: silver.clean_crm_cust_info

ID    | Tên          | Email              | Số ĐT      | Ngày ĐK    | Trạng thái | Quality Score
------|--------------|--------------------|-----------|-----------|-----------|--------------
C001  | Nguyen van a | nguyenvana@gm.com  | 0901234567| 2024-01-15| active    | 1.00
C002  | Tran thi b   | tranthib@gmail.com | 0905551234| 2024-01-15| active    | 1.00
```

✅ **Kết quả:**
- Chỉ còn 2 records (từ 4 records ban đầu)
- Không còn duplicates
- Format đã chuẩn hóa
- Tất cả dữ liệu đều hợp lệ
- Quality Score = 1.00 (100% chất lượng)

---

## ⭐ LAYER 3: GOLD - Kho Dữ Liệu Business

### Tại Sao Cần Gold Layer?

Silver Layer đã sạch rồi, nhưng:
- Dữ liệu vẫn nằm rải rác ở nhiều bảng
- Chưa có mối quan hệ rõ ràng
- Chưa tối ưu cho phân tích
- Chưa có lịch sử thay đổi

Gold Layer giải quyết vấn đề này bằng **Star Schema**.

### Star Schema Là Gì?

Hãy tưởng tượng một cửa hàng:
- **Fact (Sự kiện):** Mỗi lần bán hàng = 1 transaction
- **Dimensions (Chiều):** Thông tin về khách hàng, sản phẩm, thời gian

```
         Khách Hàng
         (Ai mua?)
              │
              │
    Thời Gian │         Sản Phẩm
    (Khi nào?)│         (Mua gì?)
              │              │
              ▼              │
         ┌─────────┐        │
         │  SALES  │◄───────┘
         │ (Bán)   │
         └─────────┘
         
    Mỗi lần bán hàng ghi lại:
    - Ai mua? (customer_key)
    - Mua gì? (product_key)
    - Khi nào? (date_key)
    - Bao nhiêu? (quantity, amount)
```

### Ví Dụ Thực Tế

#### Dimension: dim_customers (Thông tin khách hàng)
```
customer_key | customer_id | Tên          | Thành phố | Khu vực | Từ ngày    | Đến ngày   | Hiện tại?
-------------|-------------|--------------|-----------|---------|-----------|-----------|----------
1            | C001        | Nguyen Van A | Hanoi     | North   | 2024-01-15| 2024-03-20| Không (0)
2            | C001        | Nguyen Van A | HCM       | South   | 2024-03-21| 9999-12-31| Có (1)
3            | C002        | Tran Thi B   | Danang    | Central | 2024-01-15| 9999-12-31| Có (1)
```

**Giải thích:**
- Customer C001 chuyển từ Hanoi sang HCM ngày 21/03/2024
- Hệ thống giữ lại lịch sử: 
  - Record 1: Thông tin cũ (Hanoi) - Hiện tại = Không
  - Record 2: Thông tin mới (HCM) - Hiện tại = Có
- Khi phân tích, có thể biết:
  - Transactions trước 21/03 → Customer ở Hanoi
  - Transactions sau 21/03 → Customer ở HCM

#### Dimension: dim_products (Thông tin sản phẩm)
```
product_key | product_id | Tên sản phẩm | Danh mục | Giá      | Từ ngày    | Đến ngày   | Hiện tại?
------------|------------|--------------|----------|---------|-----------|-----------|----------
1           | P100       | iPhone 15    | Phone    | 20000000| 2024-01-01| 2024-06-30| Không (0)
2           | P100       | iPhone 15    | Phone    | 18000000| 2024-07-01| 9999-12-31| Có (1)
3           | P200       | MacBook Pro  | Laptop   | 35000000| 2024-01-01| 9999-12-31| Có (1)
```

**Giải thích:**
- iPhone 15 giảm giá từ 20tr xuống 18tr ngày 01/07/2024
- Hệ thống giữ lại lịch sử giá

#### Dimension: dim_date (Thông tin thời gian)
```
date_key | Ngày đầy đủ | Năm | Quý | Tháng | Ngày | Thứ    | Cuối tuần?
---------|------------|-----|-----|-------|------|--------|----------
20240115 | 2024-01-15 | 2024| Q1  | 01    | 15   | Monday | Không
20240116 | 2024-01-16 | 2024| Q1  | 01    | 16   | Tuesday| Không
20240120 | 2024-01-20 | 2024| Q1  | 01    | 20   | Saturday| Có
```

#### Fact: fact_sales (Dữ liệu bán hàng)
```
sales_key | date_key | customer_key | product_key | Số lượng | Đơn giá  | Tổng tiền
----------|----------|--------------|-------------|---------|---------|----------
1         | 20240115 | 1            | 1           | 2       | 20000000| 40000000
2         | 20240320 | 1            | 1           | 1       | 20000000| 20000000
3         | 20240321 | 2            | 1           | 1       | 20000000| 20000000
4         | 20240701 | 2            | 2           | 3       | 18000000| 54000000
```

**Giải thích:**
- Sales 1: Ngày 15/01, Customer C001 (ở Hanoi) mua 2 iPhone, giá 20tr
- Sales 2: Ngày 20/03, Customer C001 (vẫn ở Hanoi) mua 1 iPhone
- Sales 3: Ngày 21/03, Customer C001 (đã chuyển HCM) mua 1 iPhone
- Sales 4: Ngày 01/07, Customer C002 mua 3 iPhone, giá đã giảm xuống 18tr

### Phân Tích Dữ Liệu

#### Query 1: Tổng doanh thu theo khách hàng
```sql
SELECT 
    c.customer_id,
    c.Tên,
    c.Thành_phố,
    SUM(s.Tổng_tiền) as Tổng_doanh_thu
FROM fact_sales s
JOIN dim_customers c ON s.customer_key = c.customer_key
WHERE c.Hiện_tại = 1  -- Chỉ lấy thông tin hiện tại
GROUP BY c.customer_id, c.Tên, c.Thành_phố;

Kết quả:
customer_id | Tên          | Thành phố | Tổng doanh thu
------------|--------------|-----------|---------------
C001        | Nguyen Van A | HCM       | 60,000,000
C002        | Tran Thi B   | Danang    | 54,000,000
```

#### Query 2: Doanh thu theo tháng
```sql
SELECT 
    d.Năm,
    d.Tháng,
    SUM(s.Tổng_tiền) as Doanh_thu
FROM fact_sales s
JOIN dim_date d ON s.date_key = d.date_key
GROUP BY d.Năm, d.Tháng
ORDER BY d.Năm, d.Tháng;

Kết quả:
Năm  | Tháng | Doanh thu
-----|-------|------------
2024 | 01    | 40,000,000
2024 | 03    | 40,000,000
2024 | 07    | 54,000,000
```

#### Query 3: Phân tích theo khu vực (sử dụng lịch sử)
```sql
-- Doanh thu khi customer ở Hanoi vs HCM
SELECT 
    c.Thành_phố,
    SUM(s.Tổng_tiền) as Doanh_thu
FROM fact_sales s
JOIN dim_customers c ON s.customer_key = c.customer_key
JOIN dim_date d ON s.date_key = d.date_key
WHERE c.customer_id = 'C001'
  AND d.Ngày_đầy_đủ BETWEEN c.Từ_ngày AND c.Đến_ngày
GROUP BY c.Thành_phố;

Kết quả:
Thành phố | Doanh thu
----------|------------
Hanoi     | 60,000,000  (Sales 1 + 2)
HCM       | 20,000,000  (Sales 3)
```

---

## 🔄 Luồng Xử Lý Hoàn Chỉnh

### Timeline: Từ Khi Khách Hàng Mua Hàng Đến Khi Xuất Hiện Trên Dashboard

```
📅 Thứ 2, 10:00 AM - Khách hàng mua hàng tại cửa hàng
    └─ Transaction được ghi vào CRM System
    └─ Transaction ID: TXN001
    └─ Customer: C001, Product: P100, Amount: 20,000,000 VND

⏰ Thứ 2, 11:00 AM - Batch Ingestion (Bronze)
    └─ Hệ thống tự động chạy sp_load_bronze
    └─ Extract dữ liệu từ CRM
    └─ Load vào bronze.crm_sales_details
    └─ Dữ liệu: Raw, chưa xử lý
    └─ Status: ✅ Loaded to Bronze

⏰ Thứ 2, 12:00 PM - Data Cleansing (Silver)
    └─ Hệ thống tự động chạy sp_load_silver
    └─ Validate: Customer C001 tồn tại? ✓
    └─ Validate: Product P100 tồn tại? ✓
    └─ Validate: Amount > 0? ✓
    └─ Standardize: Date format, Amount format
    └─ Load vào silver.clean_crm_sales_details
    └─ Quality Score: 1.00 (100%)
    └─ Status: ✅ Cleaned to Silver

⏰ Thứ 2, 01:00 PM - Data Modeling (Gold)
    └─ Hệ thống tự động chạy sp_load_gold
    └─ Lookup customer_key từ dim_customers
    └─ Lookup product_key từ dim_products
    └─ Lookup date_key từ dim_date
    └─ Insert vào fact_sales
    └─ Status: ✅ Modeled to Gold

⏰ Thứ 2, 01:30 PM - Business Intelligence
    └─ Power BI dashboard tự động refresh
    └─ Transaction xuất hiện trong report "Doanh thu hôm nay"
    └─ Manager có thể xem và phân tích
    └─ Status: ✅ Available for Analysis

📊 Tổng thời gian: 3.5 giờ từ khi transaction xảy ra đến khi xuất hiện trên dashboard
```

### Data Lineage (Dòng Dõi Dữ Liệu)

```
🔍 Tracking Transaction TXN001:

Source System (CRM)
    transaction_id: TXN001
    customer_id: C001
    product_id: P100
    amount: 20000000
    transaction_date: 2024-01-15 10:00:00
    ↓
Bronze Layer
    Table: bronze.crm_sales_details
    transaction_id: TXN001
    _batch_id: B20240115_1100
    _load_timestamp: 2024-01-15 11:00:00
    ↓
Silver Layer
    Table: silver.clean_crm_sales_details
    transaction_id: TXN001
    quality_score: 1.00
    _silver_processed_timestamp: 2024-01-15 12:00:00
    ↓
Gold Layer
    Table: gold.fact_sales
    sales_key: 12345
    transaction_id: TXN001
    date_key: 20240115
    customer_key: 1
    product_key: 1
    total_amount: 20000000
    created_timestamp: 2024-01-15 13:00:00
    ↓
Business Intelligence
    Power BI Dashboard: "Sales Report"
    Chart: "Daily Revenue"
    Value: 20,000,000 VND
```

---

## 📈 So Sánh 3 Layer

| Tiêu chí | Bronze | Silver | Gold |
|----------|--------|--------|------|
| **Mục đích** | Lưu trữ thô | Làm sạch | Phân tích |
| **Data Quality** | 0% | 80-90% | 95-99% |
| **Duplicates** | Có | Không | Không |
| **NULL values** | Có | Xử lý | Xử lý |
| **Format** | Không chuẩn | Chuẩn | Chuẩn |
| **Schema** | Giống source | Normalized | Star Schema |
| **Lịch sử** | Không | Không | Có (SCD Type 2) |
| **Người dùng** | Data Engineers | Data Engineers, Data Scientists | Business Analysts, BI Users |
| **Query speed** | Chậm | Trung bình | Nhanh |
| **Storage size** | Lớn | Trung bình | Nhỏ |

---

## 🎯 Lợi Ích Của Kiến Trúc 3 Layer

### 1. Separation of Concerns (Tách biệt trách nhiệm)
- Bronze: Chỉ lo lưu trữ
- Silver: Chỉ lo làm sạch
- Gold: Chỉ lo phân tích

### 2. Flexibility (Linh hoạt)
- Thay đổi logic làm sạch? → Chỉ cần sửa Silver
- Thay đổi mô hình phân tích? → Chỉ cần sửa Gold
- Bronze luôn giữ nguyên

### 3. Reprocessing (Xử lý lại)
- Nếu có lỗi ở Silver/Gold → Có thể chạy lại từ Bronze
- Không cần extract lại từ source

### 4. Data Quality (Chất lượng dữ liệu)
- Mỗi layer có quality check riêng
- Dễ dàng track và improve quality

### 5. Performance (Hiệu suất)
- Gold layer được tối ưu cho query
- BI tools query nhanh hơn

### 6. Compliance (Tuân thủ)
- Bronze giữ dữ liệu gốc cho audit
- Đáp ứng yêu cầu pháp lý

---

## 🚀 Kết Luận

Kiến trúc 3 layer (Bronze-Silver-Gold) giống như một dây chuyền sản xuất:

1. **Bronze:** Nhận nguyên liệu thô từ nhà cung cấp
2. **Silver:** Xử lý và làm sạch nguyên liệu
3. **Gold:** Tạo thành sản phẩm hoàn thiện

Mỗi layer có vai trò riêng, và tất cả đều quan trọng để tạo ra một Data Pipeline hiệu quả!

---

## 📚 Đọc Thêm

- [Kiến Trúc Chi Tiết](./architecture_diagram.md)
- [Luồng Biến Đổi Dữ Liệu (Technical)](./data_transformation_flow.md)
- [Data Flow - Bronze](./data_flow_bronze.md)
- [Data Flow - Silver](./data_flow_silver.md)
- [Data Flow - Gold](./data_flow_gold.md)
