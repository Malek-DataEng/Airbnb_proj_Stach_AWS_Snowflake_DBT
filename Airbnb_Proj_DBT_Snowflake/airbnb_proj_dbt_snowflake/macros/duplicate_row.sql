{% macro duplicate_row(column_partition, column_order) %}
    row_number() over (partition by {{ column_partition }} order by {{ column_order }} desc) = 1
{% endmacro %}