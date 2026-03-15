
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select host_id
from AIRBNB.BRONZE.bronze_hosts
where host_id is null



  
  
      
    ) dbt_internal_test