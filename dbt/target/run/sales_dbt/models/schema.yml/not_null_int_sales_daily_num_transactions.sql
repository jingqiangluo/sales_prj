
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select num_transactions
from "sales_db"."analytics"."int_sales_daily"
where num_transactions is null



  
  
      
    ) dbt_internal_test