SELECT * 
FROM {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10
-- for this test to pass NONE of the rows returned should have a minimum_nights value less than 1, if any rows are returned the test will fail and the results will be stored in the _test_failures schema for debugging purposes