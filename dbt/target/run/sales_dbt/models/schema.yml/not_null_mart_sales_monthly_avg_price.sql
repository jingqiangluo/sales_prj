
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_price
from "sales_db"."analytics"."mart_sales_monthly"
where avg_price is null



  
  
      
    ) dbt_internal_test