{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    )
}}

select 
    booking_id,
    listing_id,
    booking_date,
    booking_amount,
    nights_booked,
    cleaning_fee,
    service_fee,
{{ multiply('booking_amount', 'nights_booked') }} + cleaning_fee + service_fee as total_booking_amount ,
booking_status,
created_at,

from {{ ref('bronze_bookings') }}

