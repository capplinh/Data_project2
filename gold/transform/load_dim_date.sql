-- =============================================
-- Load Dimension Date - MySQL Version (Simplified)
-- Populate date dimension for 10 years
-- =============================================

-- Set recursion depth higher
SET SESSION cte_max_recursion_depth = 5000;

-- Insert dates using recursive CTE
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, week_of_year,
    is_weekend, fiscal_year, fiscal_quarter, fiscal_month
)
WITH RECURSIVE date_range AS (
  SELECT DATE('2020-01-01') as date_val
  UNION ALL
  SELECT DATE_ADD(date_val, INTERVAL 1 DAY)
  FROM date_range
  WHERE date_val < '2030-12-31'
)
SELECT 
    DATE_FORMAT(date_val, '%Y%m%d') as date_key,
    date_val as full_date,
    YEAR(date_val) as year,
    QUARTER(date_val) as quarter,
    MONTH(date_val) as month,
    DATE_FORMAT(date_val, '%M') as month_name,
    DAY(date_val) as day,
    DAYOFWEEK(date_val) - 1 as day_of_week,
    DATE_FORMAT(date_val, '%W') as day_name,
    WEEK(date_val, 3) as week_of_year,
    CASE WHEN DAYOFWEEK(date_val) IN (1,7) THEN TRUE ELSE FALSE END as is_weekend,
    YEAR(date_val) as fiscal_year,
    QUARTER(date_val) as fiscal_quarter,
    MONTH(date_val) as fiscal_month
FROM date_range
ON DUPLICATE KEY UPDATE date_key=date_key;

-- Reset recursion depth
SET SESSION cte_max_recursion_depth = 1000;
