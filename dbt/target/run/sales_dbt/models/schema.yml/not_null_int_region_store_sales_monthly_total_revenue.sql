
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_revenue
from "sales_db"."analytics"."int_region_store_sales_monthly"
where total_revenue is null



  
  
      
    ) dbt_internal_test