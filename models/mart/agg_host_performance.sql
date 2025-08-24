{{ 
    config(
        materialized='table'
    ) 
}}

-- Truy vấn tổng hợp hiệu suất của host
SELECT
  h.host_id,
  COUNT(DISTINCT l.listing_id) AS listing_count,
  {{ review_count() }} AS review_count,
  AVG(l.price) AS avg_price_per_listing,
  {{ avg_sentiment() }} AS avg_sentiment
FROM {{ ref('dim_hosts') }} h
{{ join_hosts_to_reviews() }}  -- Kết nối bảng hosts với bảng reviews
GROUP BY h.host_id
HAVING COUNT(DISTINCT l.listing_id) > 0  -- Loại bỏ host không có listing hợp lệ