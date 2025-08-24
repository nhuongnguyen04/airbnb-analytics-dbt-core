{% macro avg_sentiment() %}
AVG(CASE 
  WHEN review_sentiment = 'positive' THEN 1 
  WHEN review_sentiment = 'neutral' THEN 0 
  WHEN review_sentiment = 'negative' THEN -1 
  ELSE 0 END)  -- Tái sử dụng điểm số sentiment
{% endmacro %}