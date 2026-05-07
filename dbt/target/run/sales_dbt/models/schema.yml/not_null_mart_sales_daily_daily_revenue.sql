
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select daily_revenue
from "sales_db"."analytics"."mart_sales_daily"
where daily_revenue is null



  
  
      
    ) dbt_internal_test