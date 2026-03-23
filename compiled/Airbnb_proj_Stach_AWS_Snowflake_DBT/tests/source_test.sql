-- Tester la validité des données source 
-- Elle retourne tous les bookings avec un booking_amount ou nights_booked inférieur ou égal à 0
-- Si elle retourne des lignes, cela signifie que le source n'est pas fonctionnel

select 
    booking_amount 
from 
    AIRBNB.BRONZE.bronze_bookings
where booking_amount <= 0
or nights_booked <= 0