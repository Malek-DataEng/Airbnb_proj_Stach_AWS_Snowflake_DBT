
{{ config(materialized='incremental') }}

select 
    *,
    TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD"T"HH24:MI:SS') as etl_loaded_at

from {{source('staging', 'bookings')}}

where {{ incremental('created_at') }} 

qualify {{ duplicate_row('booking_id', 'created_at') }}
