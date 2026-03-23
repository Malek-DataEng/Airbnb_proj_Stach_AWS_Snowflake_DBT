

select 
    listing_id, 
    host_id, 
    
    upper(trim(property_type))
 as property_type, 
    room_type, 
    
    lower(trim(city))
 as city,
    
    lower(trim(country))
 as country,
    accommodates,
    bedrooms,
    bathrooms,
    
    round(bedrooms / accommodates , 2)
 as bedroom_density,  -- nalyser confort vs capacité
    price_per_night, 
    
    round(price_per_night / accommodates , 2)
 as price_per_person,  -- Prix par personne pour analyse comparative juste
    
    case
        when price_per_night < 75 then 'BUDGET'
        when price_per_night BETWEEN 75 AND 200 then 'MID_RANGE'
        else 'LUXURY'
    end as price_per_night_tag
 , -- Catégorisation du prix
    created_at as listing_created_at,
    etl_loaded_at

from AIRBNB.BRONZE.bronze_listings

where 
    
        listing_created_at > ( select coalesce(max(listing_created_at), '1900-01-01') from AIRBNB.silver.silver_listings )
    
 -- incremental avec unique_key ne supporte pas duplicate key et pas compatible avec SCD-2