{% macro join_listings_to_hosts( ) %}
    LEFT JOIN {{ ref('dim_listings') }} l ON h.host_id = l.host_id
{% endmacro %}

{% macro join_hosts_to_reviews( ) %}
    LEFT JOIN {{ ref('dim_listings') }} l ON h.host_id = l.host_id
    LEFT JOIN {{ ref('fact_reviews') }} f ON l.listing_id = f.listing_id
{% endmacro %}
