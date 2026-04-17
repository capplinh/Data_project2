# Data Flow - Silver Layer

## Tổng Quan

Silver Layer làm sạch, validate và chuẩn hóa dữ liệu từ Bronze Layer.

## Transformation Process

### 1. Data Cleansing
- Remove duplicates
- Handle NULL values
- Trim whitespaces
- Standardize formats

### 2. Data Validation
- Email format validation
- Phone number formatting
- Date range checks
- Business rule validation

### 3. Data Quality Scoring
Mỗi record được gán quality score từ 0.0 đến 1.0

## Stored Procedure: sp_load_silver

**Logic chính:**
1. Clean CRM customer data
2. Clean CRM product data
3. Clean CRM sales data
4. Clean ERP customer data
5. Clean ERP location data
6. Clean ERP product data
7. Calculate quality scores
8. Log execution results

## Quality Rules

- Email: Valid format (xxx@domain.com)
- Phone: Numeric only, 10-15 digits
- Dates: Valid date range
- Amounts: Non-negative values
