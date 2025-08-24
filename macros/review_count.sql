{% macro review_count() %}
COUNT(review_id)  -- Tái sử dụng đếm
{% endmacro %}