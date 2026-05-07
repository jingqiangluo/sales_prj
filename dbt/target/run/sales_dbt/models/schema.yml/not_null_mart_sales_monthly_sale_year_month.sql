
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sale_year_month
from "sales_db"."analytics"."mart_sales_monthly"
where sale_year_month is null



  
  
      
    ) dbt_internal_test