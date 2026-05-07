

SELECT
    sale_date,
    store_id,
    region,
    COUNT(DISTINCT transaction_id) AS num_transactions,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales,
    AVG(price) AS avg_unit_price,
    SUM(CASE WHEN discount_pct > 0 THEN 1 ELSE 0 END) AS discounted_transactions
FROM "sales_db"."analytics"."stg_sales"
GROUP BY sale_date, store_id, region