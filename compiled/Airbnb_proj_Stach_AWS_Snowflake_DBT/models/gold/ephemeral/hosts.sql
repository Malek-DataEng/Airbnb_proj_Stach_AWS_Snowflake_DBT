

with hosts as (
select 
    host_id,
    host_name,
    host_since,
    is_superhost,
    host_response_segment,
    host_created_at,
    etl_loaded_at
from AIRBNB.silver.silver_hosts
)

select * from hosts