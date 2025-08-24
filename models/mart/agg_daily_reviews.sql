{{ 
    config(
        materialized='table'
    ) 
}}

-- Truy vấn tổng hợp đánh giá theo ngày
SELECT
  date_key,
  {{ review_count() }} AS review_count,
  COUNT(DISTINCT listing_id) AS unique_listing_count,
  COUNT(DISTINCT host_id) AS unique_host_count,
  {{ avg_sentiment() }} AS avg_sentiment
FROM {{ ref('fact_reviews') }}
GROUP BY date_key