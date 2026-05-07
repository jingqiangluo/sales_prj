
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        sale_date
    from "sales_db"."analytics"."mart_sales_daily"
    group by sale_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test