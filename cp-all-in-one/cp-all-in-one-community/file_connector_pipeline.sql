-- Drop stream sequence matters due to dependencies
DROP CONNECTOR IF EXISTS csv_file_source;
DROP STREAM IF EXISTS reviews_csv_silver;
DROP STREAM IF EXISTS reviews_csv_raw;

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
  'schema.generation.enabled'     = 'true',
  'schema.generation.key.fields'  = 'id',
  'key.converter'                 = 'org.apache.kafka.connect.storage.StringConverter',
  'value.converter'               = 'org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable'= 'false'
);

CREATE STREAM IF NOT EXISTS reviews_csv_raw (
    value VARCHAR
) WITH (
    KAFKA_TOPIC='US_hotel_reviews_csv',
    VALUE_FORMAT='KAFKA',
    PARTITIONS=1
);

CREATE STREAM IF NOT EXISTS reviews_csv_silver WITH (
  VALUE_FORMAT='JSON' 
) AS
SELECT 
    SPLIT(value, ',')[1] AS hotel_id,
    SPLIT(value, ',')[2] AS address,
    SPLIT(value, ',')[3] AS categories,
    SPLIT(value, ',')[4] AS primary_categories,
    SPLIT(value, ',')[5] AS city,
    SPLIT(value, ',')[6] AS country,
    CAST(SPLIT(value, ',')[7] AS DOUBLE) AS latitude,
    CAST(SPLIT(value, ',')[8] AS DOUBLE) AS longitude,
    SPLIT(value, ',')[9] AS hotel_name,
    SPLIT(value, ',')[10] AS postal_code,
    SPLIT(value, ',')[11] AS province,
    SPLIT(value, ',')[12] AS reviews_date,
    CAST(SPLIT(value, ',')[13] AS INT) AS reviews_rating,
    SPLIT(value, ',')[14] AS reviews_text,
    SPLIT(value, ',')[15] AS reviews_title,
    SPLIT(value, ',')[16] AS reviews_user_city,
    SPLIT(value, ',')[17] AS reviews_username
FROM reviews_csv_raw
WHERE SPLIT(value, ',')[1] != 'id' -- Skip CSV header row if present
EMIT CHANGES;
