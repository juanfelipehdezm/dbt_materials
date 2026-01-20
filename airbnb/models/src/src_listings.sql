WITH raw_listings AS (
    --SELECT * FROM RAW.RAW_LISTINGS
    SELECT * FROM {{ source('airbnb', 'listings') }} -- points to the source defined in sources.yml
)

SELECT
    id as listing_id, 
    name as listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price as price_str,
    created_at,
    updated_at
FROM 
    raw_listings