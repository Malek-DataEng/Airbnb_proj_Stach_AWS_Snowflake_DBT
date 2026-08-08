

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
    -- price_per_night, bedroom_density et price_per_person sont calcules en silver
    -- puis etaient jetes ici. Une dimension logement sans prix ne permet ni de
    -- comparer les biens, ni d'expliquer le chiffre d'affaires par le tarif.
    price_per_night,
    bedroom_density,
    price_per_person,
    price_per_night_tag,
    listing_created_at,
    etl_loaded_at
from AIRBNB.silver.silver_listings
)

select * from listings