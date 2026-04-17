# Data Catalog

## Tổng Quan

Data Catalog cung cấp metadata và documentation cho tất cả các data assets trong Data Lakehouse.

## Bronze Layer Tables

### CRM System

#### crm_cust_info
- **Mô tả:** Thông tin khách hàng từ CRM
- **Source:** CRM Database
- **Refresh:** Daily at 2:00 AM
- **Row Count:** ~500K
- **Key Columns:** cust_id (PK)

#### crm_prd_info
- **Mô tả:** Catalog sản phẩm từ CRM
- **Source:** CRM Database
- **Refresh:** Daily at 2:00 AM
- **Row Count:** ~50K
- **Key Columns:** product_id (PK)

#### crm_sales_details
- **Mô tả:** Chi tiết giao dịch bán hàng
- **Source:** CRM Database
- **Refresh:** Hourly
- **Row Count:** ~10M
- **Key Columns:** transaction_id (PK)

### ERP System

#### erp_cust_az12
- **Mô tả:** Master data khách hàng từ ERP
- **Source:** ERP Database
- **Refresh:** Daily at 3:00 AM
- **Row Count:** ~300K
- **Key Columns:** customer_code (PK)

#### erp_loc_a101
- **Mô tả:** Thông tin địa điểm và region
- **Source:** ERP Database
- **Refresh:** Weekly
- **Row Count:** ~5K
- **Key Columns:** location_id (PK)

#### erp_px_cat_g1v2
- **Mô tả:** Phân loại và giá sản phẩm
- **Source:** ERP Database
- **Refresh:** Daily at 3:00 AM
- **Row Count:** ~40K
- **Key Columns:** product_code (PK)

## Silver Layer Tables

### Data Quality Rules

- **Completeness:** Required fields không được NULL
- **Uniqueness:** Primary keys phải unique
- **Validity:** Email format, phone format validation
- **Consistency:** Cross-system data reconciliation
- **Accuracy:** Business rule validation

### Quality Score Calculation

```
Score = (filled_fields / total_fields) * weight_factor
- 1.0: Excellent (all fields populated)
- 0.8: Good (most fields populated)
- 0.5: Fair (minimal fields populated)
- 0.0: Poor (critical fields missing)
```

## Gold Layer Tables

### Star Schema Model

#### Dimensions

**dim_customers**
- **Type:** SCD Type 2
- **Grain:** One row per customer per change
- **Sources:** Silver CRM + ERP customer data
- **Refresh:** Daily
- **Row Count:** ~600K

**dim_products**
- **Type:** SCD Type 2
- **Grain:** One row per product per change
- **Sources:** Silver CRM + ERP product data
- **Refresh:** Daily
- **Row Count:** ~60K

**dim_date**
- **Type:** Static dimension
- **Grain:** One row per day
- **Range:** 2020-01-01 to 2030-12-31
- **Refresh:** Annually
- **Row Count:** ~4K

#### Facts

**fact_sales**
- **Type:** Transaction fact
- **Grain:** One row per sales transaction
- **Sources:** Silver sales data + Gold dimensions
- **Refresh:** Hourly
- **Row Count:** ~10M
- **Measures:** quantity, unit_price, discount_amount, tax_amount, total_amount

## Data Lineage

```
CRM/ERP → Bronze → Silver → Gold → BI Reports
```

## Business Definitions

- **Customer:** Cá nhân hoặc tổ chức mua sản phẩm
- **Product:** Hàng hóa hoặc dịch vụ được bán
- **Transaction:** Một giao dịch bán hàng hoàn chỉnh
- **Total Amount:** Tổng giá trị = (quantity × unit_price) - discount + tax

## Data Retention Policy

- **Bronze:** 2 years
- **Silver:** 3 years
- **Gold:** 5 years
