



select
    1
from AIRBNB.BRONZE.bronze_bookings

where not(nights_booked >= 1)

