-- =============================================
-- Load Sample Data for Testing - MySQL
-- =============================================

-- Sample CRM Customer Data
INSERT INTO crm_cust_info (cust_id, cust_name, email, phone, address, city, state, zip_code, registration_date, customer_type, status) VALUES
('CUST001', 'Nguyen Van A', 'nguyenvana@email.com', '0901234567', '123 Le Loi', 'Ho Chi Minh', 'HCM', '70000', '2023-01-15', 'RETAIL', 'ACTIVE'),
('CUST002', 'Tran Thi B', 'tranthib@email.com', '0912345678', '456 Nguyen Hue', 'Ha Noi', 'HN', '10000', '2023-02-20', 'RETAIL', 'ACTIVE'),
('CUST003', 'Le Van C', 'levanc@email.com', '0923456789', '789 Tran Hung Dao', 'Da Nang', 'DN', '50000', '2023-03-10', 'WHOLESALE', 'ACTIVE'),
('CUST004', 'Pham Thi D', 'phamthid@email.com', '0934567890', '321 Hai Ba Trung', 'Ho Chi Minh', 'HCM', '70000', '2023-04-05', 'RETAIL', 'ACTIVE'),
('CUST005', 'Hoang Van E', 'hoangvane@email.com', '0945678901', '654 Le Duan', 'Can Tho', 'CT', '90000', '2023-05-12', 'WHOLESALE', 'ACTIVE');

-- Sample CRM Product Data
INSERT INTO crm_prd_info (product_id, product_name, description, category, brand, unit_price, currency, status) VALUES
('PROD001', 'Laptop Dell XPS 13', 'High-end ultrabook', 'ELECTRONICS', 'Dell', 25000000, 'VND', 'ACTIVE'),
('PROD002', 'iPhone 15 Pro', 'Latest smartphone', 'ELECTRONICS', 'Apple', 30000000, 'VND', 'ACTIVE'),
('PROD003', 'Samsung Galaxy S24', 'Android flagship', 'ELECTRONICS', 'Samsung', 22000000, 'VND', 'ACTIVE'),
('PROD004', 'Sony WH-1000XM5', 'Noise cancelling headphones', 'AUDIO', 'Sony', 8000000, 'VND', 'ACTIVE'),
('PROD005', 'iPad Air', 'Tablet device', 'ELECTRONICS', 'Apple', 15000000, 'VND', 'ACTIVE');

-- Sample CRM Sales Data
INSERT INTO crm_sales_details (transaction_id, transaction_date, cust_id, product_id, quantity, unit_price, discount_pct, tax_amount, total_amount, payment_method) VALUES
('TXN001', '2024-01-15 10:30:00', 'CUST001', 'PROD001', 1, 25000000, 5.0, 2375000, 26125000, 'CREDIT_CARD'),
('TXN002', '2024-01-16 14:20:00', 'CUST002', 'PROD002', 1, 30000000, 0.0, 3000000, 33000000, 'BANK_TRANSFER'),
('TXN003', '2024-01-17 09:15:00', 'CUST003', 'PROD003', 2, 22000000, 10.0, 3960000, 43560000, 'CASH'),
('TXN004', '2024-01-18 16:45:00', 'CUST004', 'PROD004', 1, 8000000, 0.0, 800000, 8800000, 'CREDIT_CARD'),
('TXN005', '2024-01-19 11:30:00', 'CUST005', 'PROD005', 1, 15000000, 5.0, 1425000, 15675000, 'BANK_TRANSFER'),
('TXN006', '2024-02-01 13:20:00', 'CUST001', 'PROD004', 2, 8000000, 5.0, 1520000, 16720000, 'CREDIT_CARD'),
('TXN007', '2024-02-05 10:10:00', 'CUST002', 'PROD001', 1, 25000000, 0.0, 2500000, 27500000, 'BANK_TRANSFER'),
('TXN008', '2024-02-10 15:30:00', 'CUST003', 'PROD002', 1, 30000000, 10.0, 2700000, 29700000, 'CASH'),
('TXN009', '2024-02-15 09:45:00', 'CUST004', 'PROD005', 1, 15000000, 0.0, 1500000, 16500000, 'CREDIT_CARD'),
('TXN010', '2024-02-20 14:00:00', 'CUST005', 'PROD003', 1, 22000000, 5.0, 2090000, 22990000, 'BANK_TRANSFER');

-- Sample ERP Customer Data
INSERT INTO erp_cust_az12 (customer_code, customer_full_name, contact_email, contact_phone, business_type, credit_limit, account_status) VALUES
('CUST001', 'Nguyen Van A', 'nguyenvana@email.com', '0901234567', 'INDIVIDUAL', 50000000, 'ACTIVE'),
('CUST002', 'Tran Thi B', 'tranthib@email.com', '0912345678', 'INDIVIDUAL', 30000000, 'ACTIVE'),
('CUST003', 'Le Van C', 'levanc@email.com', '0923456789', 'BUSINESS', 100000000, 'ACTIVE'),
('CUST004', 'Pham Thi D', 'phamthid@email.com', '0934567890', 'INDIVIDUAL', 40000000, 'ACTIVE'),
('CUST005', 'Hoang Van E', 'hoangvane@email.com', '0945678901', 'BUSINESS', 80000000, 'ACTIVE');

-- Sample ERP Location Data
INSERT INTO erp_loc_a101 (location_id, location_name, address_line1, address_line2, city, state, country, postal_code, region) VALUES
('LOC001', 'Ho Chi Minh Branch', '123 Nguyen Hue', 'District 1', 'Ho Chi Minh', 'HCM', 'Vietnam', '70000', 'SOUTH'),
('LOC002', 'Ha Noi Branch', '456 Ba Dinh', 'Ba Dinh District', 'Ha Noi', 'HN', 'Vietnam', '10000', 'NORTH'),
('LOC003', 'Da Nang Branch', '789 Bach Dang', 'Hai Chau District', 'Da Nang', 'DN', 'Vietnam', '50000', 'CENTRAL'),
('LOC004', 'Can Tho Branch', '321 Ninh Kieu', 'Ninh Kieu District', 'Can Tho', 'CT', 'Vietnam', '90000', 'SOUTH');

-- Sample ERP Product Catalog
INSERT INTO erp_px_cat_g1v2 (product_code, product_desc, category_code, category_name, subcategory, unit_cost, stock_qty, reorder_level) VALUES
('PROD001', 'Laptop Dell XPS 13', 'CAT001', 'ELECTRONICS', 'Computers', 20000000, 50, 10),
('PROD002', 'iPhone 15 Pro', 'CAT001', 'ELECTRONICS', 'Smartphones', 25000000, 100, 20),
('PROD003', 'Samsung Galaxy S24', 'CAT001', 'ELECTRONICS', 'Smartphones', 18000000, 80, 15),
('PROD004', 'Sony WH-1000XM5', 'CAT002', 'AUDIO', 'Headphones', 6000000, 150, 30),
('PROD005', 'iPad Air', 'CAT001', 'ELECTRONICS', 'Tablets', 12000000, 60, 12);

-- Verify data loaded
SELECT '========================================' as '';
SELECT 'Bronze Layer Data Loaded Successfully!' as '';
SELECT '========================================' as '';
SELECT '' as '';
SELECT 'CRM Customers' as table_name, COUNT(*) as records FROM crm_cust_info
UNION ALL SELECT 'CRM Products', COUNT(*) FROM crm_prd_info
UNION ALL SELECT 'CRM Sales', COUNT(*) FROM crm_sales_details
UNION ALL SELECT 'ERP Customers', COUNT(*) FROM erp_cust_az12
UNION ALL SELECT 'ERP Locations', COUNT(*) FROM erp_loc_a101
UNION ALL SELECT 'ERP Products', COUNT(*) FROM erp_px_cat_g1v2;
