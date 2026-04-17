# Data Lakehouse — Medallion Architecture (Bronze / Silver / Gold)

> **Portfolio Project · Data Engineering**  
> *On-Premise pipeline mô phỏng luồng dữ liệu ERP/CRM thực tế — từ raw ingestion đến Star Schema sẵn sàng cho BI*

---

## Project Overview

Dự án xây dựng **Data Lakehouse** theo kiến trúc 3-layer (Medallion Architecture) với **2 implementations song song**:

| | PostgreSQL Version | MySQL Version |
|---|---|---|
| **Mục đích** | Production-ready pipeline | Learning & rapid testing |
| **Source** | ERP PostgreSQL (Docker) | Sample SQL scripts |
| **Dữ liệu** | ~91,000 rows giả lập | ~70 rows mẫu |
| **Highlight** | Python ELT pipeline, SCD Type 2 | Stored procedures, ETL log |
| **Kết nối BI** | Metabase / Superset | Power BI / Tableau |

---

## Architecture

### PostgreSQL Version (Production)

```
┌──────────────────────────────┐
│   ERP Source Database         │   PostgreSQL 16 (Docker :5433)
│   users / orders / products   │   ~91,000 rows có dirty data
└─────────────┬────────────────┘
              │  pipeline_bronze.py (Python)
              │  • Server-side cursor — chunked read
              │  • Ép toàn bộ cột → TEXT
              │  • Gắn _batch_id / _ingested_at
              ▼
┌──────────────────────────────┐
│   BRONZE SCHEMA               │   Raw data · Append-only
│   bronze.users / orders ...   │   Data Quality ≈ 0%
└─────────────┬────────────────┘
              │  silver_transform.sql (SQL thuần)
              │  • Dedup    : ROW_NUMBER() PARTITION BY
              │  • NULL     : COALESCE → 'Unknown'
              │  • Date     : parse_date() — 6 formats
              │  • Email    : regex validate
              │  • Flag lỗi thay vì xoá
              ▼
┌──────────────────────────────┐
│   SILVER SCHEMA               │   Cleaned & standardized
│   silver.users / orders ...   │   Data Quality ≈ 85%
└─────────────┬────────────────┘
              │  gold_transform.sql (SQL thuần)
              │  • Star Schema modeling
              │  • SCD Type 2 (dim_users, dim_products)
              │  • dim_date generate 2020–2030
              │  • Business Views cho BI
              ▼
┌──────────────────────────────┐
│   GOLD SCHEMA                 │   Star Schema · BI-ready
│   dim_* + fact_*              │   Data Quality ≈ 97%
└──────────────────────────────┘
```

### MySQL Version (Learning/Testing)

```
load_sample_data.sql
        ↓
Bronze Tables (MySQL)
        ↓  silver/transform/*.sql  +  sp_load_silver
Silver Tables (MySQL)  ←── ETL log / audit trail
        ↓  gold/transform/*.sql   +  sp_load_gold
Gold Tables (MySQL — Star Schema)
```

---

## Cấu trúc Project

```
Data_project2/
│
├── docker-compose.yml            # PostgreSQL ERP container
├── .env.example                  # Template credentials
├── .gitignore
│
├── Python (PostgreSQL pipeline)
│   ├── generate_source_data.py      # Sinh ~91K rows có dirty data
│   └── pipeline_bronze.py           # Extract Source → Bronze
│
├── bronze/
│   ├── ddl/
│   │   ├── crm_tables.sql           # Schema CRM tables
│   │   └── erp_tables.sql           # Schema ERP tables
│   ├── procedures/
│   │   └── sp_load_bronze.sql       # Stored procedure load Bronze
│   └── verify_bronze.sql            # Kiểm tra Bronze sau ingest
│
├── silver/
│   ├── ddl/
│   │   └── silver_tables.sql        # Schema Silver tables
│   ├── transform/
│   │   ├── clean_crm_cust_info.sql
│   │   ├── clean_crm_prd_info.sql
│   │   ├── clean_crm_sales_details.sql
│   │   ├── clean_erp_cust_az12.sql
│   │   ├── clean_erp_loc_a101.sql
│   │   └── clean_erp_px_cat_g1v2.sql
│   ├── procedures/
│   │   └── sp_load_silver.sql
│   └── silver_transform.sql         # Master script (PostgreSQL)
│
├── gold/
│   ├── ddl/
│   │   ├── dim_customers.sql        # SCD Type 2
│   │   ├── dim_products.sql         # SCD Type 2
│   │   ├── dim_date.sql
│   │   └── fact_sales.sql
│   ├── transform/
│   │   ├── load_dim_customers.sql
│   │   ├── load_dim_products.sql
│   │   ├── load_dim_date.sql
│   │   └── load_fact_sales.sql
│   ├── procedures/
│   │   └── sp_load_gold.sql
│   └── gold_transform.sql           # Master script (PostgreSQL)
│
├── testing/
│   ├── load_sample_data.sql         # Sample data MySQL
│   ├── test_bronze.sql
│   ├── test_silver.sql
│   ├── test_gold.sql
│   ├── test_queries.sql             # Sample BI queries
│   ├── quick_test.sh
│   └── run_all_tests.sh
│
├── setup/
│   ├── create_schemas.sql           # Tạo schemas bronze/silver/gold
│   └── deploy_all.sql               # Deploy toàn bộ một lệnh
│
└── docs/
    ├── QUICKSTART.md
    ├── TESTING_GUIDE.md
    ├── PROJECT_OVERVIEW.md
    ├── GIAI_THICH_3_LAYER.md        # Giải thích Medallion (Tiếng Việt)
    ├── MYSQL_EXECUTION_GUIDE.md
    ├── MYSQL_VS_GLUE.md
    ├── architecture_diagram.md
    ├── data_catalog.md
    └── data_flow_bronze_silver_gold.md
```

---

## Yêu cầu môi trường

| Tool | Version | Dùng cho |
|---|---|---|
| Docker Desktop | ≥ 4.x | PostgreSQL container |
| Python | ≥ 3.10 | Pipeline script |
| MySQL | ≥ 8.0 | MySQL version |
| psql client | any | Verify trực tiếp (tuỳ chọn) |

```bash
pip install faker psycopg2-binary python-dotenv tabulate
```

---

## Quick Start

### PostgreSQL Version

```bash
# 1. Clone & chuẩn bị
git clone https://github.com/capplinh/Data_project2.git
cd Data_project2
cp .env.example .env

# 2. Khởi động Source DB
docker compose up -d

# 3. Sinh dữ liệu giả lập
python generate_source_data.py

# 4. Bronze — Extract & Load
python pipeline_bronze.py

# 5. Silver — Clean & Standardize (idempotent)
cat silver/silver_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source

# 6. Gold — Star Schema (idempotent)
cat gold/gold_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source

# 7. Kiểm tra kết quả
docker exec -it erp_source_db psql -U erp_user -d erp_source \
  -c "SELECT * FROM gold.vw_top_products;"
```

### MySQL Version

```bash
# Tạo database
mysql -u root -p -e "CREATE DATABASE data_lakehouse;"
mysql -u root -p data_lakehouse < setup/create_schemas.sql

# Chạy pipeline một lệnh
./demo_pipeline.sh

# Hoặc từng bước
mysql -u root -p data_lakehouse < testing/load_sample_data.sql
mysql -u root -p data_lakehouse < silver/transform/clean_crm_cust_info.sql
mysql -u root -p data_lakehouse < gold/transform/load_dim_customers.sql
```

> 📖 Xem chi tiết: [docs/QUICKSTART.md](docs/QUICKSTART.md) | [docs/MYSQL_EXECUTION_GUIDE.md](docs/MYSQL_EXECUTION_GUIDE.md)

---

## Data Model — Gold Layer (Star Schema)

```
                   ┌──────────────┐
                   │   dim_date   │
                   │  date_key PK │
                   │  year        │
                   │  quarter     │
                   │  month_name  │
                   │  is_weekend  │
                   └──────┬───────┘
                          │
┌─────────────────┐  ┌────▼─────────────┐  ┌──────────────────┐
│   dim_users     │  │   fact_orders    │  │  dim_products    │
│  (SCD Type 2)  │  │──────────────────│  │  (SCD Type 2)   │
│─────────────────│◄─│ order_key     PK │─►│──────────────────│
│ user_key     PK │  │ date_key      FK │  │ product_key   PK │
│ user_id      NK │  │ user_key      FK │  │ product_id    NK │
│ full_name       │  │ product_key   FK │  │ product_name     │
│ city / country  │  │ quantity         │  │ category         │
│ effective_from  │  │ unit_price       │  │ unit_price       │
│ effective_to    │  │ discount_amount  │  │ effective_from   │
│ is_current      │  │ total_amount     │  │ effective_to     │
└─────────────────┘  └──────────────────┘  │ is_current       │
                                            └──────────────────┘
```

### SCD Type 2 — Lịch sử thay đổi

```sql
-- User chuyển từ HCM → Hà Nội → vẫn phân tích được quá khứ
user_key=1  user_id=42  city='Hồ Chí Minh'  effective_from='2022-01-01'  effective_to='2024-05-31'  is_current=false
user_key=9  user_id=42  city='Hà Nội'        effective_from='2024-06-01'  effective_to=NULL          is_current=true
```

---

## Dirty Data & Data Quality

Dữ liệu giả lập được nhúng lỗi **có chủ đích** để chứng minh năng lực xử lý ở Silver:

| Loại lỗi | Tỉ lệ | Cột bị ảnh hưởng | Giải pháp Silver |
|---|---|---|---|
| Duplicate rows | ~5% | users, orders, transactions | `ROW_NUMBER() PARTITION BY` |
| NULL values | ~8% | phone, city, supplier | `COALESCE` → `'Unknown'` |
| Sai format ngày | ~3% | order_date, txn_date | Custom `parse_date()` — 6 formats |
| Giá trị âm | ~2% | amount, unit_price | `CASE WHEN > 0` → NULL |
| Email sai format | ~1% | users.email | Regex validate |

```sql
-- Xem báo cáo chất lượng dữ liệu
SELECT * FROM silver.vw_data_quality_report;
```

**Kết quả thực tế sau Silver:**

| Entity | Bronze | Silver | Bad Email | Bad Date | Bad Amount |
|---|---|---|---|---|---|
| users | 4,200 | 853 | 15 | 6 | — |
| products | 1,000 | 500 | — | — | 11 |
| orders | 16,800 | 7,736 | — | 50 | 156 |
| transactions | 18,900 | 9,450 | — | 56 | 185 |

---

## Sample BI Queries

```sql
-- Doanh thu theo tháng & danh mục sản phẩm
SELECT * FROM gold.vw_revenue_by_month_category WHERE year = 2024;

-- Top 10 sản phẩm bán chạy
SELECT * FROM gold.vw_top_products;

-- Phân tích buyer theo thành phố
SELECT * FROM gold.vw_user_orders_by_city ORDER BY total_revenue DESC;

-- Hiệu suất phương thức thanh toán
SELECT * FROM gold.vw_payment_summary;
```

---

## Testing

```bash
# PostgreSQL — verify từng layer
docker exec -i erp_source_db psql -U erp_user -d erp_source -f bronze/verify_bronze.sql

# MySQL — full test suite
./testing/run_all_tests.sh

# Sample BI queries
mysql -u root -p data_lakehouse < testing/test_queries.sql
```

---

## Incremental Load (SCD Type 2)

```bash
# Chạy định kỳ (daily batch)
python pipeline_bronze.py                        # append batch mới

cat silver/silver_transform.sql | \
  docker exec -i erp_source_db psql -U erp_user -d erp_source   # re-process Silver

docker exec -it erp_source_db psql -U erp_user -d erp_source \
  -c "CALL gold.upsert_dim_users();"             # SCD Type 2 upsert
```

---

## Roadmap

| Phase | Status | Nội dung |
|---|---|---|
| **Phase 1 — On-Premise** | ✅ Done | Bronze · Silver · Gold · MySQL & PostgreSQL · Idempotent pipeline |
| **Phase 2 — Enhancement** | 🔄 In Progress | Incremental load · CDC · Data quality monitoring |
| **Phase 3 — Orchestration** | 📋 Planned | Apache Airflow · Error handling · Alerting |
| **Phase 4 — Cloud** | 📋 Planned | AWS S3 · Glue · Iceberg · QuickSight |

---

## Troubleshooting

**PostgreSQL container không start:**
```bash
docker logs erp_source_db
docker compose down && docker compose up -d
```

**Silver/Gold script báo lỗi "depends on":**
```bash
# Script đã có idempotent — chạy thẳng, không cần drop thủ công
cat silver/silver_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source
```

**MySQL connection error:**
```bash
brew services list | grep mysql    # macOS
sudo systemctl status mysql        # Linux
```

**REGEXP_REPLACE lỗi trên MySQL < 8.0:**
```sql
-- Dùng REPLACE thay thế
REPLACE(REPLACE(phone, '-', ''), ' ', '')
```

---

## Kỹ năng thể hiện qua project

- **Medallion Architecture** — Bronze / Silver / Gold separation of concerns
- **ELT Pattern** — Load raw trước, transform sau trong warehouse
- **Data Quality Engineering** — đánh flag lỗi, không xoá, truy vết qua `_batch_id`
- **SCD Type 2** — lịch sử thay đổi dimension (users, products)
- **Star Schema Design** — tối ưu cho OLAP, BI tools
- **Idempotent Pipeline** — chạy lại N lần, row count không đổi
- **Python Pipeline** — chunked read, batch insert, logging, `.env` config
- **Stored Procedures** — ETL log, audit trail (MySQL)
- **Docker** — reproducible database environment
- **Advanced SQL** — Window functions, CTEs, custom functions, `GENERATE_SERIES`

---

## Documentation

| File | Nội dung |
|---|---|
| [QUICKSTART.md](docs/QUICKSTART.md) | Chạy nhanh trong 5 phút |
| [TESTING_GUIDE.md](docs/TESTING_GUIDE.md) | Test từng layer |
| [GIAI_THICH_3_LAYER.md](docs/GIAI_THICH_3_LAYER.md) | Giải thích Medallion (Tiếng Việt) |
| [MYSQL_EXECUTION_GUIDE.md](docs/MYSQL_EXECUTION_GUIDE.md) | Hướng dẫn MySQL version |
| [MYSQL_VS_GLUE.md](docs/MYSQL_VS_GLUE.md) | So sánh on-premise vs AWS Glue |
| [data_catalog.md](docs/data_catalog.md) | Schema & column definitions |

---

*Python · PostgreSQL · MySQL · Docker · SQL*