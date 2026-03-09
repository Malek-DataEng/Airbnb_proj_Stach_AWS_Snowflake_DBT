{% macro trim_upper(column_name) %}
    upper(trim({{ column_name }}))
{% endmacro %}