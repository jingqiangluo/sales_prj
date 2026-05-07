
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price
from "sales_db"."analytics"."stg_sales"
where price is null



  
  
      
    ) dbt_internal_test