-- =============================================
-- Deploy All - Master Script
-- Mục đích: Deploy toàn bộ database objects
-- =============================================

-- Step 1: Create schemas
\i setup/create_schemas.sql

-- Step 2: Create Bronze tables
\i bronze/ddl/crm_tables.sql
\i bronze/ddl/erp_tables.sql

-- Step 3: Create Silver tables
\i silver/ddl/silver_tables.sql

-- Step 4: Create Gold tables
\i gold/ddl/dim_customers.sql
\i gold/ddl/dim_products.sql
\i gold/ddl/dim_date.sql
\i gold/ddl/fact_sales.sql

-- Step 5: Create stored procedures
\i bronze/procedures/sp_load_bronze.sql
\i silver/procedures/sp_load_silver.sql
\i gold/procedures/sp_load_gold.sql

-- Deployment completed
SELECT 'Database deployment completed successfully!' as status;
