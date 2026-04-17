#!/bin/bash

# =============================================
# Script Tự Động Test Toàn Bộ Pipeline
# =============================================

DB_NAME="data_lakehouse"
DB_USER="root"

echo "=========================================="
echo "Data Lakehouse - Automated Testing Script"
echo "=========================================="
echo ""

# Nhập password
read -sp "Nhập MySQL password cho user $DB_USER: " DB_PASS
echo ""

# Function để chạy SQL file
run_sql() {
    echo "▶ Đang chạy: $1"
    mysql -u $DB_USER -p$DB_PASS $DB_NAME < $1
    if [ $? -eq 0 ]; then
        echo "✓ Hoàn thành: $1"
    else
        echo "✗ Lỗi khi chạy: $1"
        exit 1
    fi
    echo ""
}

# Bước 1: Tạo database
echo "Bước 1: Tạo database..."
mysql -u $DB_USER -p$DB_PASS -e "DROP DATABASE IF EXISTS $DB_NAME; CREATE DATABASE $DB_NAME;"
echo "✓ Database đã tạo"
echo ""

# Bước 2: Deploy schema
echo "Bước 2: Deploy database schema..."
run_sql "setup/deploy_all.sql"

# Bước 3: Load sample data
echo "Bước 3: Load sample data vào Bronze..."
run_sql "testing/load_sample_data.sql"

# Bước 4: Test Bronze
echo "Bước 4: Test Bronze Layer..."
run_sql "testing/test_bronze.sql"

# Bước 5: Transform Silver
echo "Bước 5: Transform Silver Layer..."
run_sql "silver/transform/clean_crm_cust_info.sql"
run_sql "silver/transform/clean_crm_prd_info.sql"
run_sql "silver/transform/clean_crm_sales_details.sql"
run_sql "silver/transform/clean_erp_cust_az12.sql"
run_sql "silver/transform/clean_erp_loc_a101.sql"
run_sql "silver/transform/clean_erp_px_cat_g1v2.sql"

# Bước 6: Test Silver
echo "Bước 6: Test Silver Layer..."
run_sql "testing/test_silver.sql"

# Bước 7: Load Gold
echo "Bước 7: Load Gold Layer..."
run_sql "gold/transform/load_dim_date.sql"
run_sql "gold/transform/load_dim_customers.sql"
run_sql "gold/transform/load_dim_products.sql"
run_sql "gold/transform/load_fact_sales.sql"

# Bước 8: Test Gold
echo "Bước 8: Test Gold Layer..."
run_sql "testing/test_gold.sql"

# Bước 9: Run Star Schema queries
echo "Bước 9: Test Star Schema Queries..."
run_sql "testing/test_queries.sql"

echo ""
echo "=========================================="
echo "✓ TẤT CẢ TESTS ĐÃ HOÀN THÀNH THÀNH CÔNG!"
echo "=========================================="
echo ""
echo "Để xem kết quả chi tiết, chạy:"
echo "mysql -u $DB_USER -p $DB_NAME"
