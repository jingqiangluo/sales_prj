
  
    

  create  table "sales_db"."analytics"."mart_sales_daily__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    sale_year_month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(num_transactions) AS monthly_transactions,
    ROUND(AVG(avg_unit_price)::numeric, 2) AS avg_price
FROM "sales_db"."analytics"."int_region_store_sales_monthly"
GROUP BY sale_year_month
ORDER BY sale_year_month
  );
  