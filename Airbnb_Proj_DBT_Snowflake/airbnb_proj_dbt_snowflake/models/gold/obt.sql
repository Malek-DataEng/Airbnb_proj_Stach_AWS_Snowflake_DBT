{% set configs = [ 
    { 
        "table": "airbnb.silver.silver_bookings",
        "columns": "silver_bookings.*",
        "alisa": "silver_bookings"
    },
    {
        "table": "airbnb.silver.silver_listings",
        "columns": "silver_listings.HOST_ID ,silver_listings.PROPERTY_TYPE , silver_listings.ROOM_TYPE , silver_listings.CITY ,silver_listings.COUNTRY , silver_listings.ACCOMMODATES , silver_listings.BEDROOMS , silver_listings.BATHROOMS , silver_listings.bedroom_density, silver_listings.PRICE_PER_NIGHT , silver_listings.PRICE_PER_PERSON ,silver_listings.PRICE_PER_NIGHT_TAG , silver_listings.CREATED_AT as listing_created_at, silver_listings.etl_loaded_at as listing_etl_loaded_at",
        "alisa": "silver_listings",
        "join_condition": "silver_bookings.listing_id = silver_listings.listing_id"
    },
    {
        "table": "airbnb.silver.silver_hosts",
        "columns": "silver_hosts.HOST_NAME , silver_hosts.host_since,silver_hosts.host_tenure_years,silver_hosts.IS_SUPERHOST,silver_hosts.superhost_flag,silver_hosts.RESPONSE_RATE,silver_hosts.host_response_segment ,silver_hosts.CREATED_AT as host_created_at, silver_hosts.etl_loaded_at as host_etl_loaded_at",
        "alisa": "silver_hosts",
        "join_condition": "silver_listings.host_id = silver_hosts.host_id"
    }

    ] %}

select
    {% for config in configs %}
        {{ config.columns }}{% if not loop.last %},{% endif %}
    {% endfor %}
from
{% for config in configs %}
    {% if loop.first %}
        {{ config.table }}  
    {% else %}
        left join {{ config.table }} on {{ config.join_condition }}
    {% endif %}
{% endfor %}
