-- test: pour creer pull request afin de tester le le workflow CI

select
        book.booking_id, 
        book.booking_date, --2025-03-10T00:00:00
        book.booking_year, 
        book.booking_month, 
        book.booking_week, 
        book.booking_amount, 
        book.nights_booked, 
        book.booking_price_per_night, 
        book.cleaning_fee, 
        book.service_fee, 
        book.total_fees, 
        book.total_booking_value, 
        book.net_revenue, 
        book.created_at, 
        book.etl_loaded_at,    
        d_list.listing_id,    --70
        d_host.host_id        --42
    
from airbnb.silver.silver_bookings  book
    
left join airbnb.gold.dim_listings  d_list on book.listing_id = d_list.listing_id
and book.created_at between d_list.dbt_valid_from and d_list.dbt_valid_to
left join airbnb.gold.dim_hosts  d_host on d_list.host_id = d_host.host_id
and book.created_at between d_host.dbt_valid_from and d_host.dbt_valid_to

--------------------------------------------------
-------------------------------------------------
select * from airbnb.gold.dim_hosts

--------------------------------------------------
-------------------------------------------------
select
    
        book.booking_id, book.booking_date, book.booking_year, book.booking_month, book.booking_week, book.booking_amount, book.nights_booked, book.booking_price_per_night, book.cleaning_fee, book.service_fee, book.total_fees, book.total_booking_value, book.net_revenue, book.created_at, book.etl_loaded_at,
    
        d_list.listing_id,
    
        d_host.host_id
    
from

    
        airbnb.silver.silver_bookings  book
    

    
        left join airbnb.gold.dim_listings  d_list on book.listing_id = d_list.listing_id
      and book.created_at between d_list.dbt_valid_from and d_list.dbt_valid_to
    

    
        left join airbnb.gold.dim_hosts  d_host on d_list.host_id = d_host.host_id
        
      and book.created_at between d_host.dbt_valid_from and d_host.dbt_valid_to

--------------------------------------------------
-------------------------------------------------

SELECT * FROM AIRBNB.STAGING.BOOKINGS
 qualify {{ duplicate_row('BOOKING_id, created_at', 'created_at') }}


