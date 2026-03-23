

with listings as (
select 
    listing_id,
    host_id,
    property_type,
    room_type,
    city,
    country,
    ACCOMMODATES,
    bedrooms,
    bathrooms,
    price_per_night_tag,
    listing_created_at,
    etl_loaded_at
from AIRBNB.silver.silver_listings
)

select * from listings