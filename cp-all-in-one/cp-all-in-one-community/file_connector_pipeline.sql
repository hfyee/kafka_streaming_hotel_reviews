-- Drop stream sequence matters due to dependencies
DROP CONNECTOR IF EXISTS csv_file_source;
DROP TABLE IF EXISTS hotel_reviews_stats;
DROP STREAM IF EXISTS reviews_csv_keyed;
DROP STREAM IF EXISTS reviews_csv_raw;

-- Backticks to force ksqlDB to treat the string literally — incl. any invisible hidden characters.
-- Need PARTITIONS parameter in the WITH clause for ksqlDB to create the topic.
CREATE STREAM IF NOT EXISTS reviews_csv_raw (
    `id` VARCHAR,
    `address` VARCHAR,
    `categories` VARCHAR,
    `primaryCategories` VARCHAR,
    `city` VARCHAR,
    `country` VARCHAR,
    `latitude` DOUBLE,
    `longitude` DOUBLE,
    `name` VARCHAR,
    `postalCode` VARCHAR,
    `province` VARCHAR,
    `reviews.date` VARCHAR,
    `reviews.rating` INT,
    `reviews.text` VARCHAR,
    `reviews.title` VARCHAR,
    `reviews.userCity` VARCHAR,
    `reviews.username` VARCHAR
) WITH (
    KAFKA_TOPIC='US_hotel_reviews_csv',
    VALUE_FORMAT='JSON',
    PARTITIONS = 1
);

-- Add 'csv.escape.char' so embedded quotes and line breaks inside review text are handled correctly
-- ASCII 44 is comma and ASCII 34 is double-quote "
CREATE SOURCE CONNECTOR IF NOT EXISTS csv_file_source WITH (
  'connector.class'               = 'com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector',
  'tasks.max'                     = '1',
  'topic'                         = 'US_hotel_reviews_csv',
  'input.path'                    = '/file-data',
  'finished.path'                 = '/file-data/finished',
  'error.path'                    = '/file-data/error',
  'input.file.pattern'            = '(?i)^hotel_reviews.*\.csv$',
  'halt.on.error'                 = 'false',
  'csv.first.row.as.header'       = 'true',
  'csv.separator.char'            = '44',
  'csv.quote.char'                = '34',
  'schema.generation.enabled'     = 'true',
  'schema.generation.key.fields'  = 'id',
  'key.converter'                 = 'org.apache.kafka.connect.storage.StringConverter',
  'value.converter'               = 'org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'= 'false'
);

-- To compress multiple reviews per hotel into a unique list of hotels, 
-- create a stream with a primary key defined
CREATE STREAM IF NOT EXISTS reviews_csv_keyed
WITH (
    KAFKA_TOPIC='US_hotel_reviews_keyed',
    VALUE_FORMAT='JSON',
    KEY_FORMAT='KAFKA'
) AS
SELECT 
    `id` AS hotel_id,
    `name` AS hotel_name,
    `categories` AS categories,
    `city` AS city,
    `province` AS state,
    `postalCode` AS postal_code,
    `address` AS address,
    `reviews.date` AS reviews_date,
    `reviews.title` AS reviews_title,
    CAST(`reviews.rating` AS INT) AS reviews_rating,
    `reviews.text` AS reviews_text,
    `reviews.username` AS reviews_username,
    `reviews.userCity` AS reviews_user_city
FROM reviews_csv_raw
WHERE `id` IS NOT NULL
PARTITION BY `id`;

-- Materialise as a table (keeps only the latest record per hotel id)
CREATE TABLE hotel_reviews_stats AS
SELECT 
    hotel_id,
    LATEST_BY_OFFSET(hotel_name) AS hotel_name,
    LATEST_BY_OFFSET(city) AS city,
    LATEST_BY_OFFSET(state) AS state,
    LATEST_BY_OFFSET(postal_code) AS postal_code,
    COUNT(*) AS total_reviews,
    ROUND(AVG(CAST(reviews_rating AS DOUBLE)), 2) AS avg_rating,
    MIN(reviews_date) AS earliest_review_date,
    MAX(reviews_date) AS latest_review_date
FROM reviews_csv_keyed
GROUP BY hotel_id
EMIT CHANGES;