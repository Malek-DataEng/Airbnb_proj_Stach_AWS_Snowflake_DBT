{% set configs = [ 
    { 
        "table": ref('fact_bookings'),
        "columns": "fact_book.*",
        "alias": "fact_book"
    },
    {
        "table": ref('dim_listings'),
        "columns": "d_list.property_type, d_list.room_type, d_list.city,d_list.country,d_list.ACCOMMODATES,d_list.bedrooms,d_list.bathrooms,d_list.price_per_night_tag,d_list.listing_created_at" ,
        "alias": "d_list",
        "join_condition": "fact_book.listing_id = d_list.listing_id",
        "filter_condition": "fact_book.created_at between d_list.dbt_valid_from and d_list.dbt_valid_to"
    },
    {
        "table": ref('dim_hosts'),
        "columns": "d_host.host_name,d_host.host_since,d_host.is_superhost,d_host.host_response_segment,d_host.HOST_CREATED_AT",
        "alias": "d_host",
        "join_condition": "d_list.host_id = d_host.host_id",
         "filter_condition": "fact_book.created_at between d_host.dbt_valid_from and d_host.dbt_valid_to"
    }

    ] %}

select
    {% for config in configs %}
        {{ config.columns }}{% if not loop.last %},{% endif %}
    {% endfor %}
from
{% for config in configs %}
    {% if loop.first %}
        {{ config.table }}  {{ config.alias }}
    {% else %}
        left join {{ config.table }}  {{ config.alias }} on {{ config.join_condition }}
        and {{ config.filter_condition }}
    {% endif %}
{% endfor %}
