{{
    config(
        materialized = 'incremental',
        unique_key = 'review_id',
        incremental_strategy = 'merge'
    )
}}
select
    {{safe_cast('r.review_id', 'bigint')}} as review_id, -- PK
    {{safe_cast('r.listing_id', 'bigint')}} as listing_id, -- FK to dim_listing
    {{safe_cast('h.host_id', 'bigint')}} as host_id, -- FK to dim_host
    {{safe_cast('r.review_date', 'date')}} as review_date, 
    r.sentiment as review_sentiment,
    r.reviewer_name as reviewer_name,
    {{safe_cast('d.date_id', 'int')}} as date_key -- FK to dim_date
from {{ source('airbnb', 'reviews') }} r
left join {{ ref('dim_listings') }} l on r.listing_id = l.listing_id
left join {{ ref('dim_hosts') }} h on l.host_id = h.host_id
left join {{ ref('dim_date') }} d on r.review_date = d.date

{% if is_incremental() %}
where r.review_date > (select max(t.review_date) from {{ this }} t)
{% endif %}
