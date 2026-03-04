{{ config(materialized='ephemeral') }}

with bookings as (
select 
    booking_id,
    booking_date,
    booking_year,
    booking_month,  
    booking_week,
    booking_status,
    created_at,
    etl_loaded_at
from {{ ref('obt') }}
)

select * from bookings