{{
  config(
    materialized = 'incremental'
    )
}}

select 
    listing_id, 
    host_id, 
    {{ trim_upper('property_type') }} as property_type, 
    room_type, 
    {{ trim_lower('city') }} as city,
    {{ trim_lower('country') }} as country,
    accommodates,
    bedrooms,
    bathrooms,
    {{ divide('bedrooms', 'accommodates') }} as bedroom_density,  -- nalyser confort vs capacité
    price_per_night, 
    {{ divide('price_per_night', 'accommodates') }} as price_per_person,  -- Prix par personne pour analyse comparative juste
    {{ tag('price_per_night') }} , -- Catégorisation du prix
    created_at,
    etl_loaded_at

from {{ ref('bronze_listings') }}

where {{ incremental('created_at') }} -- incremental avec unique_key ne supporte pas duplicate key et pas compatible avec SCD-2


