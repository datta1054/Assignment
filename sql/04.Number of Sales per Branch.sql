-- Report: Number of Sales per Branch
-- Interprets "sales" as transaction rows in the fact table; also provides distinct invoice count.

SELECT
    b.branch_code,
    b.city,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT f.invoice_id) AS invoice_count
FROM silver_fact_sales f
JOIN silver_dim_branch b
  ON f.branch_key = b.branch_key
GROUP BY 1, 2
ORDER BY transaction_count DESC;
