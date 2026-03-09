{% macro trim_lower(column_name) %}
    lower(trim({{ column_name }}))
{% endmacro %}