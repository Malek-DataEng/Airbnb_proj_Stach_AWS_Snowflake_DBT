

select
    
        fact_book.*,
    
        d_list.property_type, d_list.room_type, d_list.city,d_list.country,d_list.ACCOMMODATES,d_list.bedrooms,d_list.bathrooms,d_list.price_per_night_tag,d_list.listing_created_at,
    
        d_host.host_name,d_host.host_since,d_host.is_superhost,d_host.host_response_segment,d_host.HOST_CREATED_AT
    
from

    
        AIRBNB.gold.fact_bookings  fact_book
    

    
        left join airbnb.gold.dim_listings  d_list on fact_book.listing_id = d_list.listing_id
        and fact_book.created_at between d_list.dbt_valid_from and d_list.dbt_valid_to
    

    
        left join airbnb.gold.dim_hosts  d_host on d_list.host_id = d_host.host_id
        and fact_book.created_at between d_host.dbt_valid_from and d_host.dbt_valid_to
    
