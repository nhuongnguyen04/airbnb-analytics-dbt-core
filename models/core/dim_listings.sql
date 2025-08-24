-- Cấu hình cho model dbt
-- materialized='incremental': Chỉ cập nhật dữ liệu mới thay vì toàn bộ
-- unique_key='listing_id': Khóa duy nhất để xác định bản ghi
-- incremental_strategy='merge': Sử dụng chiến lược merge để cập nhật dữ liệu
{{ config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='merge'
) }}

-- Nếu đang chạy ở chế độ incremental
{% if is_incremental() %}
    -- Truy vấn lấy thời điểm cập nhật gần nhất từ bảng hiện tại
    {% set max_updated_at_query %}
        SELECT MAX(l.updated_at) AS max_updated_at FROM {{ source('airbnb', 'listings') }} l
        LEFT JOIN {{ this }} t ON l.id = t.listing_id
        WHERE t.listing_id IS NOT NULL
    {% endset %}
    -- Thực thi truy vấn và lấy giá trị max_updated_at
    {% set results = run_query(max_updated_at_query) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else '1900-01-01' %}
{% endif %}

-- Truy vấn chính để lấy dữ liệu listings
select
    {{safe_cast('l.id', 'bigint')}} as listing_id,
    {# SPLIT_PART(l.listing_url, '//', 2): Chia URL bằng // (phân tách protocol và domain), lấy phần thứ 2 (e.g., www.airbnb.com/rooms/3176).
    SPLIT_PART(..., '/', 1): Chia phần vừa lấy bằng / và lấy phần đầu tiên (e.g., www.airbnb.com). #}
    SPLIT_PART(SPLIT_PART(l.listing_url, '//', 2), '/', 1) as listing_domain,
    COALESCE(
        CASE 
            WHEN LENGTH(TRIM(COALESCE(l.name, ''))) < 5 THEN 'Unknown Listing'  -- Thay giá trị độ dài < 5
            ELSE TRIM(COALESCE(l.name, ''))  -- Giữ giá trị hợp lệ
        END,
        'Unknown Listing'  -- Giá trị mặc định nếu NULL sau COALESCE
    ) as listing_name,
     
    l.room_type as room_type,-- Loại phòng

    -- Xử lý giá trị minimum_nights bất thường
    CASE
        WHEN {{safe_cast('l.minimum_nights', 'int')}} <= 0 THEN (SELECT AVG(minimum_nights) FROM {{ source('airbnb', 'listings') }} WHERE minimum_nights > 0)
        WHEN {{safe_cast('l.minimum_nights', 'int')}} >= 3650 THEN 3649
        ELSE {{safe_cast('l.minimum_nights', 'int')}}
    END as minimum_nights,
    {{safe_cast('h.host_id', 'bigint')}} as host_id, -- Ép kiểu host_id sang bigint

    -- Xử lý giá trị price bất thường
    CASE 
        WHEN {{safe_cast('l.price','numeric')}} <=0 THEN (SELECT AVG(price) FROM {{ source('airbnb', 'listings') }} WHERE price > 0)
        WHEN {{safe_cast('l.price','numeric')}} > 10000 THEN 9999.99
        ELSE {{safe_cast('l.price', 'numeric')}}
    END as price
from {{source('airbnb','listings') }} l
left join {{ ref('dim_hosts') }} h on l.host_id = h.host_id

-- Chỉ lấy dữ liệu mới hơn max_updated_at nếu chạy incremental
{% if is_incremental() %}
    where {{last_updated_filter(['l.created_at', 'l.updated_at'])}} > '{{ max_updated_at }}'
{% endif %}