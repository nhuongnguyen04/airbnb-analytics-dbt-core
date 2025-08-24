{{ 
    config(
        materialized='table'
    ) 
}}

-- Truy vấn tổng hợp theo loại phòng
SELECT
  room_type,
  COUNT(DISTINCT l.listing_id) AS total_listings,
  AVG(price) AS avg_price,
  {{ review_count() }} AS review_count
FROM {{ ref('dim_listings') }} l
LEFT JOIN {{ ref('fact_reviews') }} f ON l.listing_id = f.listing_id
GROUP BY room_type