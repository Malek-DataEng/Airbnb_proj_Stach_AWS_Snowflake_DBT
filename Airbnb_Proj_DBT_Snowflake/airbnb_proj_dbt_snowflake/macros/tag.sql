{% macro tag(column_name) %}
    case
        when {{ column_name }} < 100 then 'low'
        when {{ column_name }} < 500 then 'medium'
        else 'high'
    end as {{ column_name }}_tag
{% endmacro %}