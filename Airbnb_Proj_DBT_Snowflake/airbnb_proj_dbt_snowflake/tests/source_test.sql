-- This is a test to check if the source is working
-- It will return all the bookings with a booking amount less than 0
-- If it returns any rows, it means the source is not working

select 
    booking_amount 
from 
    {{ source('staging', 'bookings') }}
where booking_amount < 0