{% set configs = [ 
    { 
        "table": "airbnb.gold.obt",
        "columns": "obt.booking_id,obt.listing_id,obt.host_id,obt.booking_amount,obt.nights_booked,obt.booking_price_per_night,obt.service_fee,obt.cleaning_fee,obt.total_fees, obt.total_booking_value,obt.net_revenue,obt.accommodates,obt.price_per_night,obt.price_per_person, obt.host_tenure_years,obt.superhost_flag,obt.response_rate, obt.etl_loaded_at",
        "alisa": "obt"
    },
    {
        "table": "airbnb.gold.dim_listings",
        "columns": "",
        "alisa": "dim_listings",
        "join_condition": "obt.listing_id = dim_listings.listing_id"
    },
    {
        "table": "airbnb.gold.dim_hosts",
        "columns": "",
        "alisa": "dim_hosts",
        "join_condition": "obt.host_id = dim_hosts.host_id"
    }

    ] %}

select
    {{ configs[0]['columns'] }}
from
{% for config in configs %}
    {% if loop.first %}
        {{ config.table }}  
    {% else %}
        left join {{ config.table }} on {{ config.join_condition }}
    {% endif %}
{% endfor %}
