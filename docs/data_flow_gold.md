# Data Flow - Gold Layer

## Tổng Quan

Gold Layer tạo Star Schema model từ Silver Layer data.

## Star Schema Components

### Dimensions (SCD Type 2)
- dim_customers: Customer attributes with history
- dim_products: Product attributes with history
- dim_date: Time dimension

### Facts
- fact_sales: Sales transactions with measures

## SCD Type 2 Implementation

Khi có thay đổi trong dimension:
1. Set is_current = FALSE cho record cũ
2. Set end_date = CURRENT_DATE cho record cũ
3. Insert record mới với is_current = TRUE
4. Set effective_date = CURRENT_DATE cho record mới

## Stored Procedure: sp_load_gold

**Logic:**
1. Load/Update dim_customers (merge CRM + ERP)
2. Load/Update dim_products (merge CRM + ERP)
3. Load fact_sales (join with dimensions)
4. Handle SCD Type 2 changes
5. Log execution

## Performance Optimization

- Indexes on foreign keys
- Partitioning by date
- Materialized views for aggregations
