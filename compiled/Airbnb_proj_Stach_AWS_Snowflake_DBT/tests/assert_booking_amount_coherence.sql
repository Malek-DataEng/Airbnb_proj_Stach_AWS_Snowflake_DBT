-- Test de coherence des montants.
-- Le montant d'une reservation doit correspondre au prix a la nuit de l'annonce
-- multiplie par le nombre de nuits reservees.
--
-- HYPOTHESE METIER, a valider avant de considerer ce test comme acquis :
-- ce projet ne modelise ni promotion, ni tarif saisonnier, ni degressivite sur les
-- longs sejours. Si une de ces regles est introduite un jour, ce test devra evoluer
-- ou etre retire, sous peine de signaler des ecarts parfaitement legitimes.
--
-- Une tolerance de 5 % absorbe les arrondis, les montants etant stockes en entiers
-- dans le DDL (NUMBER sans echelle, donc NUMBER(38,0)).

with comparaison as (

    select
        b.booking_id,
        b.booking_amount,
        b.nights_booked,
        l.price_per_night,
        l.price_per_night * b.nights_booked as montant_attendu

    from AIRBNB.BRONZE.bronze_bookings b

    inner join AIRBNB.BRONZE.bronze_listings l
        on b.listing_id = l.listing_id

    where b.nights_booked >= 1
      and l.price_per_night >= 1

)

select *
from comparaison
where abs(booking_amount - montant_attendu) > montant_attendu * 0.05