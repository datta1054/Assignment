-- Report: Customer Type Spend Analysis (Normal vs Member)
-- Shows whether members spend more on average by comparing average transaction value.

WITH by_type AS (
    SELECT
        COALESCE(customer_type, 'Unknown') AS customer_type,
        COUNT(*) AS transaction_count,
        SUM(total) AS total_sales,
        AVG(total) AS avg_transaction_value
    FROM silver_fact_sales
    GROUP BY 1
)
SELECT
    customer_type,
    transaction_count,
    total_sales,
    avg_transaction_value,
    ROUND(total_sales * 1.0 / SUM(total_sales) OVER (), 4) AS sales_share
FROM by_type
ORDER BY avg_transaction_value DESC;
