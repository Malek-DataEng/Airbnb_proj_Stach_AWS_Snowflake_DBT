

with hosts as (
select
    host_id,
    host_name,
    host_since,
    -- host_tenure_years et response_rate sont calcules en silver puis etaient jetes
    -- ici, donc absents de la dimension DIM_HOSTS. Une dimension hote sans taux de
    -- reponse ni anciennete ne permet aucune analyse de la qualite de service.
    host_tenure_years,
    is_superhost,
    superhost_flag,
    response_rate,
    host_response_segment,
    host_created_at,
    etl_loaded_at
from AIRBNB.silver.silver_hosts
)

select * from hosts