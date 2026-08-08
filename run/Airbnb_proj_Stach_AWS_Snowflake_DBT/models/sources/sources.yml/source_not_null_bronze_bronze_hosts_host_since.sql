
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select host_since
from AIRBNB.BRONZE.bronze_hosts
where host_since is null



  
  
      
    ) dbt_internal_test