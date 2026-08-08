
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_per_night
from AIRBNB.BRONZE.bronze_listings
where price_per_night is null



  
  
      
    ) dbt_internal_test