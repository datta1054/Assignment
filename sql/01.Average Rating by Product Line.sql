-- Report: Average Rating by Product Line
-- Computes average customer rating per product line.

SELECT
    p.product_line_name,
    AVG(f.rating) AS avg_rating,
    COUNT(*) AS transaction_count
FROM silver_fact_sales f
JOIN silver_dim_product_line p
  ON f.product_line_key = p.product_line_key
GROUP BY 1
ORDER BY avg_rating DESC;
