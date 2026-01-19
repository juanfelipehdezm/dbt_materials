{{
    config(
        materialized='view',
        tags=['dim', 'listings']
    )
}}



WITH src_listings AS (
    SELECT * FROM {{ref('src_listings')}} -- This is a DBT specific jinja template tag to reference another model
)

SELECT
    listing_id,
    listing_name,
    room_type,
    CASE 
        WHEN minimum_nights = 0 THEN 1
        ELSE minimum_nights
    END AS minimum_nights,
    CASE 
        WHEN minimum_nights > 4 THEN 'Long Term'
        WHEN minimum_nights BETWEEN 2 AND 4 THEN 'Short Term'
        ELSE 'One Night'
    END AS stay_type,
    host_id,
    REPLACE(price_str, '$')::NUMBER(10,2) AS price,
    created_at,
    updated_at
FROM 
    src_listings
