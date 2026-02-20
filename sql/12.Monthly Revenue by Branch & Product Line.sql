WITH sales_enriched AS (
    SELECT
        f.txn_date,
        substr(f.txn_date, 1, 7) AS year_month,
        b.branch_code,
        b.city,
        p.product_line_name,
        f.total
        FROM silver_fact_sales f
        JOIN silver_dim_branch b
      ON f.branch_key = b.branch_key
        JOIN silver_dim_product_line p
      ON f.product_line_key = p.product_line_key
),
monthly AS (
    SELECT
        year_month,
        branch_code,
        city,
        product_line_name,
        SUM(total) AS revenue
    FROM sales_enriched
    GROUP BY 1,2,3,4
)
SELECT
    year_month,
    branch_code,
    city,
    product_line_name,
    revenue,
    RANK() OVER (
        PARTITION BY year_month, branch_code
        ORDER BY revenue DESC
    ) AS product_rank_in_branch_month,
    SUM(revenue) OVER (
        PARTITION BY branch_code
        ORDER BY year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue_in_branch
FROM monthly
ORDER BY year_month, branch_code, product_rank_in_branch_month;
