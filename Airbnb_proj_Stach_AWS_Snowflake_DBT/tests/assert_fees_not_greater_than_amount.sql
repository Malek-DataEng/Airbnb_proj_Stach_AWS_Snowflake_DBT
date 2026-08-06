-- Test de coherence des frais.
-- Les frais de menage et de service ne peuvent etre ni negatifs, ni superieurs
-- au montant du sejour lui-meme.
--
-- Ce test est volontairement large : il n'impose aucun ratio, parce que le ratio
-- reel releve du metier et n'est documente nulle part dans ce projet. Il ne verifie
-- qu'un invariant que personne ne peut contester, et c'est ce qui le rend sur.
--
-- Il compte, parce que silver_bookings calcule net_revenue = booking_amount - total_fees :
-- des frais aberrants produisent un revenu net negatif qui remonte jusqu'aux
-- indicateurs Gold sans qu'aucun autre test ne le signale.

select
    booking_id,
    booking_amount,
    cleaning_fee,
    service_fee,
    cleaning_fee + service_fee as total_fees

from {{ source('bronze', 'bronze_bookings') }}

where cleaning_fee < 0
   or service_fee < 0
   or (cleaning_fee + service_fee) > booking_amount
