--explore macro
select 
    listing_id, 
    host_id, 
    {{ trim_upper('property_type') }} as property_type, 
    room_type, 
    price_per_night, 
    {{ tag('price_per_night') }} ,
    accommodates
from {{ ref('bronze_listings') }}

qualify {{ duplicate_row('listing_id', 'created_at') }}

