
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        sale_year_month
    from "sales_db"."analytics"."mart_sales_monthly"
    group by sale_year_month
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test