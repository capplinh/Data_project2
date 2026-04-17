# Data Flow - Bronze Layer

## Tổng Quan

Bronze Layer là lớp đầu tiên trong kiến trúc, nhận dữ liệu thô từ các source systems.

## Source Systems

### 1. CRM System
- **Connection Type:** JDBC
- **Database:** MySQL
- **Tables:** customers, products, sales_transactions
- **Load Frequency:** Hourly for sales, Daily for master data

### 2. ERP System
- **Connection Type:** ODBC
- **Database:** Oracle
- **Tables:** customer_master, location_master, product_catalog
- **Load Frequency:** Daily at 3:00 AM

## Data Flow Process

```
┌─────────────┐
│   CRM DB    │
└──────┬──────┘
       │ Extract
       ▼
┌─────────────────┐
│  Staging Area   │
└──────┬──────────┘
       │ Load
       ▼
┌─────────────────┐
│  Bronze Tables  │
└─────────────────┘
```

## Load Strategy

### Full Load
- Truncate và reload toàn bộ table
- Sử dụng cho master data tables
- Chạy vào cuối tuần

### Incremental Load
- Load chỉ dữ liệu mới/thay đổi
- Sử dụng timestamp hoặc change data capture
- Chạy hàng giờ/hàng ngày

## Stored Procedure: sp_load_bronze

**Parameters:**
- `p_load_type`: 'FULL' hoặc 'INCREMENTAL'

**Logic:**
1. Validate connection đến source systems
2. Extract dữ liệu từ sources
3. Load vào Bronze tables
4. Log execution status
5. Error handling và retry logic

## Monitoring & Logging

- ETL execution logs trong `etl_log` table
- Alert khi load fails
- Data volume tracking
- Performance metrics
