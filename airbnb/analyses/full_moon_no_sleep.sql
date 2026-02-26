WITH fullmoon_reviews AS (
    SELECT * FROM {{ ref('mart_fullmoon_reviews') }}
)
SELECT
    is_full_moon_review,
    review_sentiment,
    COUNT(*) as reviews
FROM
    fullmoon_reviews
GROUP BY
    is_full_moon_review,
    review_sentiment
ORDER BY
    is_full_moon_review,
    review_sentiment
