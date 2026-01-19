{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        tags=['fct', 'reviews']
    )
}}

WITH raw_reviews AS (
    SELECT * FROM {{ref('src_reviews')}}
)

SELECT * FROM raw_reviews
WHERE review_text IS NOT NULL
-- Only include new records for incremental runs
-- the this variable refers to the current model being built
{% if is_incremental() %}
    AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}