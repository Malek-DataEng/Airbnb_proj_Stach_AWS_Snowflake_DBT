{% set configs = [ 
    { 
        "table": ref('silver_bookings'),
        "columns": "book.booking_id, book.booking_date, book.booking_year, book.booking_month, book.booking_week, book.booking_amount, book.nights_booked, book.booking_price_per_night, book.cleaning_fee, book.service_fee, book.total_fees, book.total_booking_value, book.net_revenue, book.created_at, book.etl_loaded_at",
        "alias": "book"
    },
    {
        "table": ref('dim_listings'),
        "columns": "d_list.listing_id",
        "alias": "d_list",
        "join_condition": "book.listing_id = d_list.listing_id",
        "filter_condition": "book.created_at between d_list.dbt_valid_from and d_list.dbt_valid_to"
    },
    {
        "table": ref('dim_hosts'),
        "columns": "d_host.host_id",
        "alias": "d_host",
        "join_condition": "d_list.host_id = d_host.host_id",
        "filter_condition": "book.created_at between d_host.dbt_valid_from and d_host.dbt_valid_to"
        
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
