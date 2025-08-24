{% macro last_updated_filter(columns) %}
    {% set expr = "greatest(" ~ columns | join(', ') ~ ")" %}
    {{ return(expr) }}
{% endmacro %}