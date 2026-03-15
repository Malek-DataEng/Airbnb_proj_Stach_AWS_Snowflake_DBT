
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from AIRBNB.BRONZE.bronze_bookings

where not(nights_booked >= 1)


  
  
      
    ) dbt_internal_test