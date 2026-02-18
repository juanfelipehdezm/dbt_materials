{% test positive_values(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} <= 0
{% endtest %}
-- this test will return all the records where the value in the specified column is less than or equal to zero, which are the "bad" records we want to identify.