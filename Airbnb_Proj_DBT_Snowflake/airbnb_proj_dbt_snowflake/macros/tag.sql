-- Catégorisation du prix
{% macro tag(column_name) %}
    case
        when {{ column_name }} < 75 then 'BUDGET'
        when {{ column_name }} BETWEEN 75 AND 200 then 'MID_RANGE'
        else 'LUXURY'
    end as {{ column_name }}_tag
{% endmacro %}
