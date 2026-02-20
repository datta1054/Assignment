-- Report: Total Sales by Payment Method
-- Aggregates total revenue (SUM(total)) by payment method from the Silver fact table.

SELECT
    COALESCE(payment, 'Unknown') AS payment_method,
    SUM(total) AS total_sales
FROM silver_fact_sales
GROUP BY 1
ORDER BY total_sales DESC;
