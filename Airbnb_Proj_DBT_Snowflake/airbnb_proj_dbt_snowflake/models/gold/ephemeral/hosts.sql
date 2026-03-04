{{ config(materialized='ephemeral') }}

with hosts as (
select 
    host_id,
    host_name,
    host_since,
    is_superhost,
    host_response_segment,
    host_created_at,
    host_etl_loaded_at
from {{ ref('obt') }}
)

select * from hosts
qualify {{duplicate_row("host_id", "host_etl_loaded_at")}}