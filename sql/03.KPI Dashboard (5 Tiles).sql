-- Supermarket Sales — KPI Dashboard (5 tiles)
-- Produces a single-row summary used for dashboard KPI cards.

SELECT
    ROUND(SUM(total), 2) AS total_sales,
    ROUND(SUM(total) * 1.0 / COUNT(*), 2) AS avg_basket,
    COUNT(*) AS transactions,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(AVG(rating), 2) AS avg_rating
FROM silver_fact_sales;
