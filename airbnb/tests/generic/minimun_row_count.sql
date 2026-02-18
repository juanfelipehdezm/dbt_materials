{% test minimum_row_count(model, min_rows) %}
  SELECT COUNT(*) AS row_count
  FROM {{ model }}
  HAVING row_count < {{ min_rows }}
{% endtest %}
-- this test will return all the records from the specified model if the total number of rows is less than the specified minimum row count, 
-- which indicates that there may be an issue with the data loading process or that the data is incomplete.