-- raw event stream 
CREATE STREAM IF NOT EXISTS reviews_stream_bronze (
    id VARCHAR KEY,
    address VARCHAR,
    categories ARRAY<STRING>,
    primary_categories ARRAY<STRING>,
    city VARCHAR,
    country VARCHAR,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    name VARCHAR,
    postal_code VARCHAR,
    province VARCHAR,
    reviews_date TIMESTAMP,
    reviews_rating INT,
    reviews_text VARCHAR,
    reviews_title VARCHAR,
    reviews_user_city VARCHAR,
    reviews_username VARCHAR
) WITH (
    KAFKA_TOPIC='US_hotel_reviews',
    VALUE_FORMAT='JSON'
);

-- filtered event stream
CREATE STREAM IF NOT EXISTS reviews_stream_silver AS 
SELECT
    id AS hotel_id,
    address,
    categories,
    primary_categories,
    city,
    country,
    latitude,
    longitude,
    name AS hotel_name,
    SUBSTRING(postal_code, 1, 5) AS postal_code, -- the first 5 digits of the postal code
    province AS state,
    CAST(reviews_date AS DATE) AS reviews_date, -- from TIMESTAMP
    reviews_rating,
    reviews_text,
    reviews_title,
    reviews_user_city,
    reviews_username
FROM reviews_stream_bronze
WHERE id IS NOT NULL 
    AND reviews_rating IS NOT NULL 
    AND reviews_text IS NOT NULL;

-- materialised state table derived from silver stream
CREATE TABLE IF NOT EXISTS hotel_summary_gold AS 
SELECT
    hotel_id,
    LATEST_BY_OFFSET(hotel_name) AS hotel_name,
    LATEST_BY_OFFSET(city) AS city,
    LATEST_BY_OFFSET(state) AS state,
    LATEST_BY_OFFSET(postal_code) AS postal_code,
    LATEST_BY_OFFSET(categories) AS categories,
    AVG(CAST(reviews_rating AS DOUBLE)) AS avg_rating,
    COUNT(*) AS total_reviews,
    MIN(reviews_date) AS earliest_review_date,
    MAX(reviews_date) AS latest_review_date
FROM reviews_stream_silver
GROUP BY hotel_id;
