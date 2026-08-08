
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test de chronologie.
-- Une reservation ne peut pas exister avant la publication de l'annonce qu'elle vise.
-- Ce test retourne les reservations fautives : s'il retourne des lignes, il echoue.
--
-- Pourquoi ce test existe : aucun test de cle ni de valeur manquante ne detecte ce cas.
-- Les identifiants sont valides, les montants sont positifs, la donnee est structurellement
-- correcte et metier-ment impossible. C'est le type d'incoherence qui ne se voit qu'ici.

select
    b.booking_id,
    b.listing_id,
    b.booking_date,
    l.created_at as listing_created_at

from AIRBNB.BRONZE.bronze_bookings b

inner join AIRBNB.BRONZE.bronze_listings l
    on b.listing_id = l.listing_id

where b.booking_date < l.created_at
  
  
      
    ) dbt_internal_test