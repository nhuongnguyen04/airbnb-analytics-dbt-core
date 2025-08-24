{{
    config(
        materialized='incremental',
        unique_key = 'host_id',
        incremental_strategy = 'merge'
    )
}}

select
    {{safe_cast('id', 'bigint')}} as host_id, -- Sử dụng macro, ép kiểu bigint
    name as host_name,
    {{safe_cast('is_superhost', 'boolean')}} as is_superhost, -- Sử dụng macro , ép kiểu boolean
    {{safe_cast('created_at', 'timestamp')}} as created_at, -- Sử dụng macro, ép kiểu timestamp
    {{safe_cast('updated_at', 'timestamp')}} as updated_at  -- Sử dụng macro, ép kiểu timestamp
from {{source('airbnb', 'hosts')}} 

{% if is_incremental() %}
where {{ last_updated_filter(['created_at', 'updated_at']) }} > (select max(updated_at) from {{ this }})
{% endif %}
