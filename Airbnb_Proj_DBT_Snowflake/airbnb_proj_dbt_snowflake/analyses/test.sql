{{ config(materialized='ephemeral') }}

with bookings as (
    select 
        booking_id,
        listing_id,
        booking_price_per_night
        
    from {{ ref('silver_bookings') }}
),

listings as (
    select 
        listing_id,
        price_per_night
    from {{ ref('silver_listings') }}
)


select 
    bookings.booking_id,
    bookings.booking_price_per_night,
    listings.price_per_night
from bookings
left join listings on bookings.listing_id = listings.listing_id