{% macro incremental(column) %}
    {% if is_incremental() %}
        {{ column }} > ( select coalesce(max({{ column }}), '1900-01-01') from {{ this }} )
    {% else %}
        1=1 -- securiser le premier run pour eviter is_incremental() = false
    {% endif %}
{% endmacro %}
