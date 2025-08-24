{{ 
    config(
        materialized='table'
    ) 
}}

-- Truy vấn tổng hợp đánh giá theo listing
SELECT
  listing_id,
  {{ review_count() }} AS review_count,
  {{ avg_sentiment() }} AS avg_sentiment,
  MAX(review_date) AS latest_review_date
FROM {{ ref('fact_reviews') }}
GROUP BY listing_id