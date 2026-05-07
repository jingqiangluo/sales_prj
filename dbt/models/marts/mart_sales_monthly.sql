{{ config(materialized="table") }}

SELECT 
    sale_year_month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(num_transactions) AS monthly_transactions,
    ROUND(AVG(avg_unit_price)::numeric, 2) AS avg_price,
    MIN(data_loaded_at) AS data_loaded_at,
    CURRENT_TIMESTAMP AS model_run_at
FROM {{ ref("int_region_store_sales_monthly") }}
GROUP BY sale_year_month
ORDER BY sale_year_month
