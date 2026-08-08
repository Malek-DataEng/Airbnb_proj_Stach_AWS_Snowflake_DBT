
    
    

with all_values as (

    select
        booking_status as value_field,
        count(*) as n_records

    from AIRBNB.BRONZE.bronze_bookings
    group by booking_status

)

select *
from all_values
where value_field not in (
    'cancelled','confirmed'
)


