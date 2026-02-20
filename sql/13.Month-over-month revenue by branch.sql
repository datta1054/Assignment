-- KPI: Month-over-month revenue by branch

WITH monthly AS (
  SELECT
    b.branch_code,
    substr(f.txn_date, 1, 7) AS year_month,
    ROUND(SUM(f.total), 2) AS revenue
  FROM silver_fact_sales f
  JOIN silver_dim_branch b
    ON b.branch_key = f.branch_key
   AND b.is_current = 1
  GROUP BY b.branch_code, substr(f.txn_date, 1, 7)
)
SELECT
  branch_code,
  year_month,
  revenue,
  LAG(revenue) OVER (PARTITION BY branch_code ORDER BY year_month) AS prev_month_revenue
FROM monthly
ORDER BY branch_code, year_month;
