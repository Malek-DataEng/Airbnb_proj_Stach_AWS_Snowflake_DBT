

select 
    host_id,
    
    upper(trim(host_name))
 as host_name,
    host_since,
    datediff(year, host_since, current_date) as host_tenure_years, --Ancienneté du host
    
    upper(trim(is_superhost))
 as is_superhost,
    case 
        when is_superhost then 1
        else 0
    end as superhost_flag, --Superhost score pondéré
    response_rate,
    case
        when response_rate >= 95 then 'ELITE'
        when response_rate >= 80 then 'GOOD'
        else 'LOW'
    end as host_response_segment, --Segmentation performance
    created_at as HOST_CREATED_AT ,
    etl_loaded_at
from AIRBNB.BRONZE.bronze_hosts

where 
    
        host_created_at > ( select coalesce(max(host_created_at), '1900-01-01') from AIRBNB.silver.silver_hosts )
    
 -- incremental avec unique_key ne supporte pas duplicate key et pas compatible avec SCD-2