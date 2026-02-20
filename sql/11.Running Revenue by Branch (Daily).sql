-- KPI: Daily revenue and running (cumulative) revenue by branch

WITH daily AS (
  SELECT
    b.branch_code,
    f.txn_date,
    SUM(f.total) AS day_revenue
  FROM silver_fact_sales f
  JOIN silver_dim_branch b
    ON b.branch_key = f.branch_key
   AND b.is_current = 1
  GROUP BY b.branch_code, f.txn_date
)
SELECT
  branch_code,
  txn_date,
  ROUND(day_revenue, 2) AS day_revenue,
  ROUND(
    SUM(day_revenue) OVER (
      PARTITION BY branch_code
      ORDER BY txn_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
  ) AS running_revenue
FROM daily
ORDER BY branch_code, txn_date;
