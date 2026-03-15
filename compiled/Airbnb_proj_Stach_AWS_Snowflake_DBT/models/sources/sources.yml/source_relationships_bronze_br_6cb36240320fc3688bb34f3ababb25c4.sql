
    
    

with child as (
    select listing_id as from_field
    from AIRBNB.BRONZE.bronze_bookings
    where listing_id is not null
),

parent as (
    select listing_id as to_field
    from AIRBNB.BRONZE.bronze_listings
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


