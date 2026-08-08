
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from AIRBNB.BRONZE.bronze_hosts

where not(host_since <= current_date())


  
  
      
    ) dbt_internal_test