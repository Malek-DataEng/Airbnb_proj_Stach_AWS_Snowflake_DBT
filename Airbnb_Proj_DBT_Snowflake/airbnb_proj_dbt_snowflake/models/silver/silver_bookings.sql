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

    --Analyse temporelle 
    year(booking_date) as booking_year,
    month(booking_date) as booking_month,
    week(booking_date) as booking_week,

    booking_amount, --revenue brut (prix total du séjour)
    nights_booked,
    {{ divide('booking_amount', 'nights_booked') }} as booking_price_per_night, --prix par nuit ,
    cleaning_fee,
    service_fee,
    cleaning_fee + service_fee as total_fees , --coût total
    total_fees + booking_amount as total_booking_value, --Revenue total booking
    booking_amount - (total_fees) as net_revenue, --revenue net
    
{{ trim_upper('booking_status') }} as booking_status,
created_at,
etl_loaded_at

from {{ ref('bronze_bookings') }}

