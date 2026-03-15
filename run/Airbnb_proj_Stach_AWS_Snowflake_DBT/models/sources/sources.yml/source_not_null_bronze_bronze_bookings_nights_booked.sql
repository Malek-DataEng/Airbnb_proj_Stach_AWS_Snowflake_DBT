
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nights_booked
from AIRBNB.BRONZE.bronze_bookings
where nights_booked is null



  
  
      
    ) dbt_internal_test