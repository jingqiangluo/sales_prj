
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select monthly_transactions
from "sales_db"."analytics"."mart_sales_monthly"
where monthly_transactions is null



  
  
      
    ) dbt_internal_test