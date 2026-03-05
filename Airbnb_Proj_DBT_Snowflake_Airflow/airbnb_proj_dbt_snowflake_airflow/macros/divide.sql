{% macro divide(a, b, precision=2) %}
    round({{ a }} / {{ b }} , {{ precision }})
{% endmacro %}
