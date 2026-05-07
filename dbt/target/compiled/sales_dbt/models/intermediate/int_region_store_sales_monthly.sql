

SELECT
    -- Grain: monthly by Store & Region Group
    sale_year_month,
    store_id,
    region,
    -- Core Metrics
    COUNT(*) AS num_transactions,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_revenue,
    AVG(price) AS avg_unit_price,
    SUM(CASE WHEN is_discounted THEN 1 ELSE 0 END) AS discounted_transactions,
    -- Customer & Payment Insights
    MODE() WITHIN GROUP (ORDER BY customer_type) AS top_customer_type,
    MODE() WITHIN GROUP (ORDER BY payment_method) AS top_payment_method,
    -- Product Insights
    COUNT(DISTINCT product_name) AS unique_products_sold,
    -- Date Context
    sale_year,
    sale_month,
    -- Metadata
    MIN(loaded_at) AS data_loaded_at,
    CURRENT_TIMESTAMP AS model_run_at
FROM "sales_db"."analytics"."stg_sales"
GROUP BY
    store_id,
    region,
    sale_year,
    sale_month,
    sale_year_month