-- KPI: Top 3 product lines per branch by revenue, with % contribution within branch

WITH pl AS (
  SELECT
    b.branch_code,
    p.product_line_name,
    ROUND(SUM(f.total), 2) AS revenue
  FROM silver_fact_sales f
  JOIN silver_dim_branch b
    ON b.branch_key = f.branch_key
   AND b.is_current = 1
  JOIN silver_dim_product_line p
    ON p.product_line_key = f.product_line_key
  GROUP BY b.branch_code, p.product_line_name
),
ranked AS (
  SELECT
    branch_code,
    product_line_name,
    revenue,
    DENSE_RANK() OVER (PARTITION BY branch_code ORDER BY revenue DESC) AS rev_rank,
    ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY branch_code), 2) AS branch_revenue_pct
  FROM pl
)
SELECT
  branch_code,
  product_line_name,
  revenue,
  rev_rank,
  branch_revenue_pct
FROM ranked
WHERE rev_rank <= 3
ORDER BY branch_code, rev_rank, revenue DESC;
