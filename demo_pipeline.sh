#!/bin/bash

# ============================================
# DEMO: CHẠY FULL DATA PIPELINE TRÊN MYSQL
# Bronze → Silver → Gold
# ============================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# MySQL credentials
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"

# Prompt for password if not set
if [ -z "$MYSQL_PASS" ]; then
    echo -e "${YELLOW}Enter MySQL password for user '$MYSQL_USER' (or press Enter if no password):${NC}"
    read -s MYSQL_PASS
    echo ""
fi

# Function to run MySQL command
run_mysql() {
    if [ -z "$MYSQL_PASS" ]; then
        mysql -u "$MYSQL_USER" -h "$MYSQL_HOST" "$@"
    else
        mysql -u "$MYSQL_USER" -p"$MYSQL_PASS" -h "$MYSQL_HOST" "$@"
    fi
}

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   DATA PIPELINE DEMO - MySQL Version${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# ============================================
# STEP 0: Setup Databases
# ============================================
echo -e "${YELLOW}[STEP 0] Creating databases...${NC}"
run_mysql -e "CREATE DATABASE IF NOT EXISTS bronze;"
run_mysql -e "CREATE DATABASE IF NOT EXISTS silver;"
run_mysql -e "CREATE DATABASE IF NOT EXISTS gold;"
echo -e "${GREEN}✓ Databases created${NC}"
echo ""

# ============================================
# STEP 1: Bronze Layer - Load Raw Data
# ============================================
echo -e "${YELLOW}[STEP 1] Loading Bronze Layer (Raw Data)...${NC}"

echo "  → Creating Bronze tables..."
run_mysql bronze < bronze/ddl/crm_tables.sql
run_mysql bronze < bronze/ddl/erp_tables.sql
echo -e "${GREEN}  ✓ Bronze tables created${NC}"

echo "  → Loading sample data..."
run_mysql < testing/load_sample_data.sql
echo -e "${GREEN}  ✓ Sample data loaded${NC}"

echo "  → Checking Bronze data..."
BRONZE_COUNT=$(run_mysql -N -e "SELECT COUNT(*) FROM bronze.crm_cust_info;")
echo -e "${GREEN}  ✓ Bronze layer: $BRONZE_COUNT records in crm_cust_info${NC}"
echo ""

# ============================================
# STEP 2: Silver Layer - Clean Data
# ============================================
echo -e "${YELLOW}[STEP 2] Processing Silver Layer (Cleaned Data)...${NC}"

echo "  → Creating Silver tables..."
run_mysql silver < silver/ddl/silver_tables.sql
echo -e "${GREEN}  ✓ Silver tables created${NC}"

echo "  → Running transformations..."
run_mysql silver < silver/transform/clean_crm_cust_info.sql
run_mysql silver < silver/transform/clean_crm_prd_info.sql
run_mysql silver < silver/transform/clean_crm_sales_details.sql
run_mysql silver < silver/transform/clean_erp_cust_az12.sql
run_mysql silver < silver/transform/clean_erp_loc_a101.sql
run_mysql silver < silver/transform/clean_erp_px_cat_g1v2.sql
echo -e "${GREEN}  ✓ Transformations completed${NC}"

echo "  → Checking Silver data..."
SILVER_COUNT=$(run_mysql -N -e "SELECT COUNT(*) FROM silver.clean_crm_cust_info;")
echo -e "${GREEN}  ✓ Silver layer: $SILVER_COUNT records in clean_crm_cust_info${NC}"
echo ""

# ============================================
# STEP 3: Gold Layer - Star Schema
# ============================================
echo -e "${YELLOW}[STEP 3] Building Gold Layer (Star Schema)...${NC}"

echo "  → Creating dimension tables..."
run_mysql gold < gold/ddl/dim_customers.sql
run_mysql gold < gold/ddl/dim_products.sql
run_mysql gold < gold/ddl/dim_date.sql
echo -e "${GREEN}  ✓ Dimension tables created${NC}"

echo "  → Creating fact tables..."
run_mysql gold < gold/ddl/fact_sales.sql
echo -e "${GREEN}  ✓ Fact tables created${NC}"

echo "  → Loading dimensions..."
run_mysql gold < gold/transform/load_dim_customers.sql
run_mysql gold < gold/transform/load_dim_products.sql
run_mysql gold < gold/transform/load_dim_date.sql
echo -e "${GREEN}  ✓ Dimensions loaded${NC}"

echo "  → Loading facts..."
run_mysql gold < gold/transform/load_fact_sales.sql
echo -e "${GREEN}  ✓ Facts loaded${NC}"

echo "  → Checking Gold data..."
GOLD_CUSTOMERS=$(run_mysql -N -e "SELECT COUNT(*) FROM gold.dim_customers;")
GOLD_PRODUCTS=$(run_mysql -N -e "SELECT COUNT(*) FROM gold.dim_products;")
GOLD_SALES=$(run_mysql -N -e "SELECT COUNT(*) FROM gold.fact_sales;")
echo -e "${GREEN}  ✓ Gold layer:${NC}"
echo -e "${GREEN}    - dim_customers: $GOLD_CUSTOMERS records${NC}"
echo -e "${GREEN}    - dim_products: $GOLD_PRODUCTS records${NC}"
echo -e "${GREEN}    - fact_sales: $GOLD_SALES records${NC}"
echo ""

# ============================================
# STEP 4: Run Tests
# ============================================
echo -e "${YELLOW}[STEP 4] Running tests...${NC}"
run_mysql < testing/test_bronze.sql
run_mysql < testing/test_silver.sql
run_mysql < testing/test_gold.sql
echo -e "${GREEN}✓ All tests passed${NC}"
echo ""

# ============================================
# STEP 5: Sample Queries
# ============================================
echo -e "${YELLOW}[STEP 5] Running sample analytics queries...${NC}"
echo ""

echo -e "${BLUE}Query 1: Top 5 Customers by Revenue${NC}"
run_mysql -t gold <<EOF
SELECT 
    dc.customer_name,
    dc.city,
    SUM(fs.total_amount) as total_revenue
FROM fact_sales fs
INNER JOIN dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.is_current = 1
GROUP BY dc.customer_name, dc.city
ORDER BY total_revenue DESC
LIMIT 5;
EOF
echo ""

echo -e "${BLUE}Query 2: Sales by Month${NC}"
run_mysql -t gold <<EOF
SELECT 
    dd.year,
    dd.month_name,
    COUNT(*) as total_transactions,
    SUM(fs.total_amount) as total_revenue
FROM fact_sales fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month_name, dd.month
ORDER BY dd.year, dd.month
LIMIT 10;
EOF
echo ""

# ============================================
# Summary
# ============================================
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}   PIPELINE COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "Data Flow Summary:"
echo -e "  Bronze (Raw):     $BRONZE_COUNT → $SILVER_COUNT records (cleaned)"
echo -e "  Silver (Clean):   $SILVER_COUNT records"
echo -e "  Gold (Business):  $GOLD_SALES sales transactions"
echo ""
echo -e "Next steps:"
echo -e "  1. Explore data: ${BLUE}mysql -u $MYSQL_USER gold${NC}"
echo -e "  2. Run custom queries: ${BLUE}mysql -u $MYSQL_USER < testing/test_queries.sql${NC}"
echo -e "  3. Read guide: ${BLUE}docs/MYSQL_EXECUTION_GUIDE.md${NC}"
echo ""
