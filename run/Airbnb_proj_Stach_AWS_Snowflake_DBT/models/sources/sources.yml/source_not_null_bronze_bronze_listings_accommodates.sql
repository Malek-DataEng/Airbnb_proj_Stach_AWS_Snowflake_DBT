
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select accommodates
from AIRBNB.BRONZE.bronze_listings
where accommodates is null



  
  
      
    ) dbt_internal_test