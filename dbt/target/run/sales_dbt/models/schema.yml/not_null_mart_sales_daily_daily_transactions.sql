
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select daily_transactions
from "sales_db"."analytics"."mart_sales_daily"
where daily_transactions is null



  
  
      
    ) dbt_internal_test