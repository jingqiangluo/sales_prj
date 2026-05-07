
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_sales
from "sales_db"."analytics"."int_sales_daily"
where total_sales is null



  
  
      
    ) dbt_internal_test