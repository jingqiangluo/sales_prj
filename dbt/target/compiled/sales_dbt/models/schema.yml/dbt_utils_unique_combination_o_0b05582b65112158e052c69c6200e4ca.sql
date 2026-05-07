





with validation_errors as (

    select
        sale_date
    from "sales_db"."analytics"."mart_sales_monthly"
    group by sale_date
    having count(*) > 1

)

select *
from validation_errors


