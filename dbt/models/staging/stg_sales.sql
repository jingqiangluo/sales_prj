{{ config(materialized='view') }}

SELECT
    -- Primary Key & Identifiers
    transaction_id,
    -- Date Handling
    sale_date::DATE AS sale_date,
    EXTRACT(YEAR FROM sale_date) AS sale_year,
    EXTRACT(MONTH FROM sale_date) AS sale_month,
    TO_CHAR(sale_date, 'YYYY-MM') AS sale_year_month,
    -- Store & Location
    store_id,
    TRIM(region) as region,
    -- Product
    TRIM(product_name) AS product_name,
    -- Metrics with cleaning
    COALESCE(quantity, 0)::INT AS quantity,
    COALESCE(price, 0)::NUMERIC(10,2) AS price,
    -- Discount handling
    COALESCE(discount_pct, 0)::NUMERIC(5,2) AS discount_pct,
    CASE
        WHEN discount_pct > 0 THEN TRUE
        ELSE FALSE
    END AS is_discounted,
    -- Final Amount (core business logic)
    total_amount,
    (quantity * price * (1 - COALESCE(discount_pct, 0)/100))::NUMERIC(12,2) AS total_slaes,
    -- Customer & Payment
    INITCAP(TRIM(COALESCE(customer_type, 'Unknown'))) AS customer_type,
    INITCAP(TRIM(COALESCE(payment_method, 'Unknown'))) AS payment_method,
    -- Metadata
    loaded_at,
    CURRENT_TIMESTAMP AS transformed_at,
    'raw.sales_raw' AS source_table
FROM raw.sales_raw