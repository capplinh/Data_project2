# Sơ Đồ Kiến Trúc Chi Tiết - Core Data Platform

## Kiến Trúc Tổng Thể (Theo Mô Hình Enterprise Data Platform)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          SECURITY & OPERATION LAYER                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐                 │
│  │ Identity Access  │  │  Infrastructure  │  │  Encryption &    │                 │
│  │   Management     │  │   as Code (IaC)  │  │  Tokenization    │                 │
│  │   (IAM/RBAC)     │  │                  │  │                  │                 │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          CORE DATA PLATFORM LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │                         ANALYTICAL LAYER                                    │   │
│  │  ┌──────────────────────────┐         ┌──────────────────────────┐        │   │
│  │  │      Forecasting         │         │  Business Intelligence   │        │   │
│  │  │   (ML/AI Predictions)    │         │   (Dashboards/Reports)   │        │   │
│  │  └──────────────────────────┘         └──────────────────────────┘        │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                            │
│                                        ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │                      DATA MANAGEMENT LAYER                                  │   │
│  │  ┌──────────────────────────┐         ┌──────────────────────────┐        │   │
│  │  │       Metadata           │         │     Data Lineage         │        │   │
│  │  │  (Data Catalog/Schema)   │         │   (Tracking/Audit)       │        │   │
│  │  └──────────────────────────┘         └──────────────────────────┘        │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                            │
│                                        ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │              PROCESSING AND TRANSFORMATION LAYER                            │   │
│  │  ┌──────────────────────────────────────────────────────────────────┐     │   │
│  │  │                        ETL Jobs                                   │     │   │
│  │  │  • Data Extraction    • Data Transformation   • Data Loading     │     │   │
│  │  └──────────────────────────────────────────────────────────────────┘     │   │
│  │  ┌──────────────────────────────────────────────────────────────────┐     │   │
│  │  │                      Orchestration                                │     │   │
│  │  │  • Workflow Scheduling  • Dependency Management  • Monitoring     │     │   │
│  │  └──────────────────────────────────────────────────────────────────┘     │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                            │
│                                        ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │                         STORAGE LAYER                                       │   │
│  │  ┌──────────────────────────────────────────────────────────────────┐     │   │
│  │  │                    Datalake Storage                               │     │   │
│  │  │  • Object Storage (S3/ADLS)  • Parquet/Delta Lake Format         │     │   │
│  │  └──────────────────────────────────────────────────────────────────┘     │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                            │
│                                        ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │                        INGESTION LAYER                                      │   │
│  │  ┌──────────────────────────────────────────────────────────────────┐     │   │
│  │  │                    Batch Ingestion                                │     │   │
│  │  │  • Scheduled Loads  • Full/Incremental  • CDC (Change Capture)   │     │   │
│  │  └──────────────────────────────────────────────────────────────────┘     │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                            │
└────────────────────────────────────────┼────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES LAYER                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐     │
│  │                    Structured Data (ERP/SAP)                              │     │
│  │  • Customer Master  • Product Catalog  • Transaction Data                 │     │
│  └──────────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Kiến Trúc 3-Layer Data Pipeline (Bronze-Silver-Gold)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES LAYER                                          │
│  ┌──────────────────┐              ┌──────────────────┐                            │
│  │   CRM System     │              │   ERP System     │                            │
│  │   (MySQL)        │              │   (Oracle/SAP)   │                            │
│  │                  │              │                  │                            │
│  │  • customers     │              │  • cust_master   │                            │
│  │  • products      │              │  • location      │                            │
│  │  • sales         │              │  • prod_catalog  │                            │
│  └────────┬─────────┘              └────────┬─────────┘                            │
└───────────┼──────────────────────────────────┼──────────────────────────────────────┘
            │                                  │
            │         INGESTION LAYER          │
            │      (Batch Ingestion)           │
            ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER (Raw Data Zone)                                  │
│                         STORAGE LAYER - Datalake                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  Schema: bronze                                                                      │
│                                                                                      │
│  📦 CRM Tables:                    📦 ERP Tables:                                   │
│  • crm_cust_info                   • erp_cust_az12                                  │
│  • crm_prd_info                    • erp_loc_a101                                   │
│  • crm_sales_details               • erp_px_cat_g1v2                                │
│                                                                                      │
│  🎯 Mục đích:                                                                        │
│  • Lưu trữ dữ liệu thô (as-is) từ source systems                                    │
│  • Không có transformation, giữ nguyên schema gốc                                   │
│  • Hỗ trợ data lineage và audit trail                                               │
│  • Immutable storage (append-only)                                                  │
│                                                                                      │
│  🔧 ETL Process: sp_load_bronze                                                     │
│  📊 Data Quality: 0% (Raw data)                                                     │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           │  PROCESSING & TRANSFORMATION LAYER
                           │  (ETL Jobs + Orchestration)
                           │
                           │  ✓ Data Cleansing
                           │  ✓ Deduplication
                           │  ✓ Validation
                           │  ✓ Standardization
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                   🥈 SILVER LAYER (Cleaned Data Zone)                               │
│                         STORAGE LAYER - Datalake                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  Schema: silver                                                                      │
│                                                                                      │
│  🧹 Cleaned Tables:                                                                  │
│  • clean_crm_cust_info             • clean_erp_cust_az12                           │
│  • clean_crm_prd_info              • clean_erp_loc_a101                            │
│  • clean_crm_sales_details         • clean_erp_px_cat_g1v2                         │
│                                                                                      │
│  🎯 Mục đích:                                                                        │
│  • Dữ liệu đã được làm sạch và validate                                            │
│  • Chuẩn hóa định dạng (dates, strings, numbers)                                   │
│  • Loại bỏ duplicates và NULL values                                               │
│  • Áp dụng business rules và data quality checks                                   │
│                                                                                      │
│  🔧 Transformations:                                                                │
│  ✓ Remove duplicates               ✓ Standardize formats                           │
│  ✓ Validate data types             ✓ Calculate quality scores                      │
│  ✓ Handle NULL values              ✓ Apply business rules                          │
│                                                                                      │
│  🔧 ETL Process: sp_load_silver                                                     │
│  📊 Data Quality: 80-90% (Cleaned & Validated)                                      │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           │  PROCESSING & TRANSFORMATION LAYER
                           │  (ETL Jobs + Orchestration)
                           │
                           │  ✓ Data Modeling (Star Schema)
                           │  ✓ Dimension Building (SCD Type 2)
                           │  ✓ Fact Aggregation
                           │  ✓ Business Logic Application
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    🥇 GOLD LAYER (Business Data Zone)                               │
│                         STORAGE LAYER - Datalake                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  Schema: gold                                                                        │
│                                                                                      │
│  ⭐ STAR SCHEMA MODEL                                                               │
│                                                                                      │
│         ┌─────────────────┐                                                         │
│         │  dim_customers  │                                                         │
│         │  (SCD Type 2)   │                                                         │
│         └────────┬────────┘                                                         │
│                  │                                                                   │
│  ┌──────────────┼──────────────┐                                                   │
│  │              │               │                                                   │
│  │              ▼               │                                                   │
│  │      ┌──────────────┐       │                                                   │
│  │      │  fact_sales  │◄──────┘                                                   │
│  │      │              │                                                            │
│  │      └──────┬───────┘                                                            │
│  │             │                                                                     │
│  ▼             ▼                                                                     │
│ ┌──────────┐ ┌──────────────┐                                                      │
│ │dim_date  │ │dim_products  │                                                      │
│ │          │ │(SCD Type 2)  │                                                      │
│ └──────────┘ └──────────────┘                                                      │
│                                                                                      │
│  🎯 Mục đích:                                                                        │
│  • Dữ liệu đã được mô hình hóa theo Star Schema                                    │
│  • Tối ưu cho Business Intelligence và Analytics                                   │
│  • Hỗ trợ SCD Type 2 cho dimension history tracking                                │
│  • Aggregated metrics và KPIs                                                      │
│                                                                                      │
│  🔧 ETL Process: sp_load_gold                                                       │
│  📊 Data Quality: 95-99% (Business-Ready)                                           │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           │  DATA MANAGEMENT LAYER
                           │  (Metadata + Data Lineage)
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         ANALYTICAL LAYER                                             │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  📊 Business Intelligence:                                                           │
│  • Power BI Dashboards                                                               │
│  • Tableau Reports                                                                   │
│  • Looker Analytics                                                                  │
│                                                                                      │
│  🤖 Forecasting & ML:                                                                │
│  • Sales Forecasting Models                                                          │
│  • Customer Segmentation                                                             │
│  • Predictive Analytics                                                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Data Quality Journey

```
┌──────────┐      ┌──────────┐      ┌──────────┐
│  BRONZE  │ ───► │  SILVER  │ ───► │   GOLD   │
│   Raw    │      │  Clean   │      │ Business │
└──────────┘      └──────────┘      └──────────┘
    0%               80-90%            95-99%
  Quality           Quality           Quality
```
