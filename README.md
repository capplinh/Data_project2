# Data Lakehouse — Medallion Architecture

> **Portfolio Project · Data Engineering**
> On-Premise pipeline mô phỏng luồng dữ liệu ERP/CRM thực tế — từ raw ingestion đến Star Schema sẵn sàng cho BI

---

## Tổng quan

Dự án xây dựng **Data Lakehouse** theo kiến trúc Medallion (Bronze → Silver → Gold) với 2 implementation song song:

| | PostgreSQL | MySQL |
|---|---|---|
| Mục đích | Production-ready pipeline | Learning & testing |
| Source | ERP PostgreSQL (Docker) | Sample SQL scripts |
| Dữ liệu | ~91,000 rows giả lập | ~70 rows mẫu |
| Điểm nổi bật | Incremental load, CDC, Airflow, SCD Type 2 | Stored procedures, ETL log |

---

## Kiến trúc hệ thống

```
[ERP Source DB — PostgreSQL Docker]
           │
           │  pipeline_incremental.py
           │  • Watermark-based — chỉ load rows mới
           │  • Retry 3x + rollback khi fail
           │  • _batch_id / _ingested_at metadata
           ▼
     [BRONZE SCHEMA]
      Raw data · Append-only · Quality ≈ 0%
           │
           │  silver_transform.sql
           │  • Dedup: ROW_NUMBER() PARTITION BY
           │  • NULL: COALESCE → 'Unknown'
           │  • Date: parse_date() — 6 formats
           │  • Email: regex validate
           ▼
     [SILVER SCHEMA]
      Cleaned & standardized · Quality ≈ 85%
           │
           │  gold_transform.sql
           │  • Star Schema
           │  • SCD Type 2
           │  • dim_date 2020–2030
           ▼
     [GOLD SCHEMA]
      Star Schema · BI-ready · Quality ≈ 97%
           │
           ▼
  [MONITORING & ALERTING]
   data_quality_monitor.py  — 15 checks
   alerting.py              — Slack webhook
   monitoring_dashboard.py  — HTML auto-refresh 30s
```

### CDC — Change Data Capture

```
MySQL binlog (ROW + FULL format)
     │
     ▼  cdc_reader.py
     ├── INSERT  → log new row
     ├── UPDATE  → log before + after
     └── DELETE  → log deleted row
```

### Orchestration — Apache Airflow

```
DAG: erp_daily_pipeline  |  schedule: 0 1 * * *  (1:00 AM daily)
     │
     ├── Task 1: bronze_incremental_load
     ├── Task 2: silver_transform
     ├── Task 3: gold_transform
     ├── Task 4: data_quality_check
     └── Task 5: pipeline_done
```

---

## Cấu trúc project

```
Data_project2/
│
├── docker-compose.yml          # PostgreSQL + Airflow
├── .env.example                # Template — KHÔNG commit .env
├── .gitignore
│
├── generate_source_data.py     # Sinh ~91K rows có dirty data
├── pipeline_bronze.py          # Full load: Source → Bronze
├── pipeline_incremental.py     # Incremental load (watermark)
├── data_quality_monitor.py     # 15 quality checks
├── alerting.py                 # Slack webhook alerts
├── cdc_reader.py               # MySQL binlog CDC
├── monitoring_dashboard.py     # Generate HTML dashboard
│
├── dags/
│   └── erp_pipeline_dag.py     # Airflow DAG
│
├── bronze/
│   ├── ddl/                    # crm_tables.sql, erp_tables.sql
│   ├── procedures/             # sp_load_bronze.sql
│   └── verify_bronze.sql
│
├── silver/
│   ├── ddl/                    # silver_tables.sql
│   ├── transform/              # clean_crm_*.sql, clean_erp_*.sql
│   ├── procedures/             # sp_load_silver.sql
│   └── silver_transform.sql    # Master script
│
├── gold/
│   ├── ddl/                    # dim_*.sql, fact_sales.sql
│   ├── transform/              # load_dim_*.sql, load_fact_*.sql
│   ├── procedures/             # sp_load_gold.sql
│   └── gold_transform.sql      # Master script
│
├── testing/
│   ├── load_sample_data.sql
│   ├── test_bronze/silver/gold.sql
│   └── run_all_tests.sh
│
└── setup/
    ├── create_schemas.sql
    └── deploy_all.sql
```

---

## Yêu cầu

| Tool | Version | Dùng cho |
|---|---|---|
| Docker Desktop | ≥ 4.x | PostgreSQL + Airflow |
| Python | ≥ 3.10 | Pipeline scripts |
| MySQL | ≥ 8.0 | MySQL version + CDC |

```bash
pip install faker psycopg2-binary python-dotenv tabulate \
            requests mysql-replication cryptography
```

---

## Chạy nhanh

### 1. Setup

```bash
git clone https://github.com/capplinh/Data_project2.git
cd Data_project2
cp .env.example .env
# Điền PG_PASSWORD, MYSQL_PASSWORD, SLACK_WEBHOOK_URL vào .env

docker compose up -d
```

### 2. PostgreSQL Pipeline

```bash
# Sinh dữ liệu
python generate_source_data.py

# Bronze — incremental load
python pipeline_incremental.py

# Silver — clean & standardize
cat silver/silver_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source

# Gold — Star Schema
cat gold/gold_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source

# Quality check
python data_quality_monitor.py

# Dashboard + Alert
python monitoring_dashboard.py && open pipeline_dashboard.html
python alerting.py
```

### 3. Airflow

```bash
open http://localhost:8080   # admin / admin
# DAGs → erp_daily_pipeline → Trigger DAG ▶
```

### 4. CDC

```bash
# Terminal 1
python cdc_reader.py

# Terminal 2 — thay đổi data, Terminal 1 sẽ hiện event real-time
mysql -u root -p -e "USE bronze; UPDATE cdc_test SET status='shipped' WHERE id=1;"
```

### 5. MySQL Pipeline

```bash
mysql -u root -p -e "DROP DATABASE IF EXISTS bronze; DROP DATABASE IF EXISTS silver; DROP DATABASE IF EXISTS gold;"
./demo_pipeline.sh
```

---

## Data Model — Gold Layer (Star Schema)

```
           [dim_date]
                │
[dim_users] ◄──[fact_orders]──► [dim_products]
(SCD Type 2)                     (SCD Type 2)
                │
        [fact_transactions]
```

### SCD Type 2

```sql
-- Khi user đổi city, row cũ đóng lại, row mới tạo ra
-- → phân tích được "tháng 3 user này ở HCM, tháng 6 chuyển Hà Nội"
user_key=1  user_id=42  city='Hồ Chí Minh'  effective_from='2022-01-01'  effective_to='2024-05-31'  is_current=false
user_key=9  user_id=42  city='Hà Nội'        effective_from='2024-06-01'  effective_to=NULL          is_current=true
```

---

## Dirty Data — nhúng có chủ đích

| Loại lỗi | Tỉ lệ | Cột | Giải pháp Silver |
|---|---|---|---|
| Duplicate rows | ~5% | users, orders, transactions | `ROW_NUMBER() PARTITION BY` |
| NULL values | ~8% | phone, city, supplier | `COALESCE → 'Unknown'` |
| Sai format ngày | ~3% | order_date, txn_date | `parse_date()` — 6 formats |
| Giá trị âm | ~2% | amount, unit_price | `CASE WHEN > 0` |
| Email sai format | ~1% | email | Regex validate |

**Kết quả thực tế:**

| Table | Bronze | Silver | Ghi chú |
|---|---|---|---|
| users | 4,200 | 853 | Dedup theo email |
| orders | 16,800 | 7,736 | Filter bad date + amount |
| products | 1,000 | 500 | Dedup |
| transactions | 18,900 | 9,450 | Dedup + filter âm |

---

## Sample BI Queries

```sql
SELECT * FROM gold.vw_revenue_by_month_category WHERE year = 2024;
SELECT * FROM gold.vw_top_products;
SELECT * FROM gold.vw_user_orders_by_city ORDER BY total_revenue DESC;
SELECT * FROM gold.vw_payment_summary;
```

---

## Troubleshooting

**PostgreSQL không start:**
```bash
docker compose down && docker compose up -d
```

**Silver/Gold — chạy lại idempotent:**
```bash
cat silver/silver_transform.sql | docker exec -i erp_source_db psql -U erp_user -d erp_source
```

**MySQL port = 0 (recovery mode):**
```bash
sudo kill -9 $(pgrep mysqld) && brew services start mysql
```

**MySQL pipeline reset:**
```bash
mysql -u root -p -e "DROP DATABASE IF EXISTS bronze; DROP DATABASE IF EXISTS silver; DROP DATABASE IF EXISTS gold;"
./demo_pipeline.sh
```

---

## Roadmap

| Phase | Status | Nội dung |
|---|---|---|
| Phase 1 — On-Premise |  Done | Bronze · Silver · Gold · MySQL & PostgreSQL · Idempotent |
| Phase 2 — Enhancement | Done | Incremental load · CDC · DQ monitor · Error handling · Alerting · Dashboard |
| Phase 3 — Orchestration |  Done | Apache Airflow · DAG daily · Retry · Graph view |
| Phase 4 — Cloud |  In Progress | AWS S3 · Glue · Iceberg · Athena · QuickSight |

---

## Kỹ năng thể hiện

- **Medallion Architecture** — Bronze / Silver / Gold separation of concerns
- **ELT Pattern** — Load raw trước, transform sau trong warehouse
- **Incremental Load** — watermark-based, chỉ load data mới
- **CDC** — MySQL binlog ROW format, detect INSERT/UPDATE/DELETE real-time
- **Data Quality** — 15 checks, flag lỗi thay vì xoá, truy vết qua `_batch_id`
- **Error Handling** — retry 3x, rollback batch, pipeline run log
- **Alerting** — Slack webhook, severity WARNING/CRITICAL
- **Monitoring** — HTML dashboard auto-refresh 30s
- **Apache Airflow** — DAG, cron schedule, dependency graph
- **SCD Type 2** — lịch sử thay đổi dimension
- **Star Schema** — tối ưu cho OLAP và BI
- **Idempotent Pipeline** — chạy lại N lần, row count không đổi
- **Docker** — reproducible environment (PostgreSQL + Airflow)
- **Advanced SQL** — Window functions, CTEs, custom functions

---

*Python · PostgreSQL · MySQL · Docker · Apache Airflow · CDC · SQL*