{{
  config(
    materialized = 'incremental'
    )
}}

select 
    host_id,
    {{ trim_upper('host_name') }} as host_name,
    host_since,
    datediff(year, host_since, current_date) as host_tenure_years, --Ancienneté du host
    {{ trim_upper('is_superhost') }} as is_superhost,
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
from {{ source('bronze', 'bronze_hosts') }}

where {{ incremental('host_created_at') }} -- incremental avec unique_key ne supporte pas duplicate key et pas compatible avec SCD-2