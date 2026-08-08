



select
    1
from AIRBNB.BRONZE.bronze_hosts

where not(host_since <= current_date())

