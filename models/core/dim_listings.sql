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
    l.name as listing_name,
    l.room_type as room_type,-- Loại phòng

    {{safe_cast('l.minimum_nights', 'int')}} as minimum_nights,
    {{safe_cast('h.host_id', 'bigint')}} as host_id, -- Ép kiểu host_id sang bigint

    {{safe_cast('l.price', 'numeric')}} as price
from {{source('airbnb','listings') }} l
left join {{ ref('dim_hosts') }} h on l.host_id = h.host_id

-- Chỉ lấy dữ liệu mới hơn max_updated_at nếu chạy incremental
{% if is_incremental() %}
    where {{last_updated_filter(['l.created_at', 'l.updated_at'])}} > '{{ max_updated_at }}'
{% endif %}