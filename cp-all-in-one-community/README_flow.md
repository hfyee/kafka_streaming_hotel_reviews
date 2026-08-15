# Folder structure
```
Working dir: ~/NYP_EDM/ITG201/assignment/kafka_streaming_hotel_reviews

File structure:
|-docker-compose.yml
|-datagen_pipeline.sql
|-file_connector_pipeline.sql
|-reviews_schema.json
|-connect-plugins
|-data/Hotel_Reviews.csv
|-finished
|-error
```

# Setup 1: Confluent Kafka Community (ksqlDB)
//docker exec -it confluent-test bash
Clone the Confluent All-in-One GitHub repo at https://github.com/confluentinc/cp-all-in-one/tree/8.2.0-post
Go to working folder cp-all-in-one/cp-all-in-one-community.

## Terminal 1 ("datagen") 
docker compose exec ksqldb-server \
  ksql-datagen \
  schema=/ksql-scripts/reviews_schema.json \
  key=id \
  topic=US_hotel_reviews \
  format=json \
  bootstrap-server=broker:29092 \
  msgRate=1

## Terminal 2 ("ksqlDB cli")
### cp-all-in-one-community configuration template does not include a standalone ksql-cli container;
### Run the interactive ksql command directly inside the server container
docker compose exec ksqldb-server ksql http://localhost:8088
ksql> RUN SCRIPT '/ksql-scripts/datagen_pipeline.sql';

### Once both commands are running with matching topic names, test the stream in ksqlDB by running:
### Prelim checks in ksql cli
-- List streams to check exact name
SHOW STREAMS;
-- List topics to ensure US_hotel_reviews is receiving messages
SHOW TOPICS;
// tell ksqlDB to read from message 0
SET 'auto.offset.reset' = 'earliest'; # default is 'latest'
PRINT 'US_hotel_reviews' FROM BEGINNING LIMIT 5;
-- Read only live new incoming records
PRINT 'US_hotel_reviews' INTERVAL 1 LIMIT 5;

### Check raw incoming stream:
SELECT * FROM reviews_stream_bronze EMIT CHANGES LIMIT 10;
SELECT id, name, city, categories, reviews_rating FROM reviews_stream_bronze EMIT CHANGES LIMIT 10;
### Check filtered/cleaned silver stream:
SELECT hotel_id, hotel_name, postal_code, categories, reviews_rating FROM reviews_stream_silver EMIT CHANGES LIMIT 10;
### Query the aggregated materialized table:
SELECT * FROM hotel_summary_gold EMIT CHANGES LIMIT 10;

# -----------------------------------------------------------------
# Setup 2: Confluent file connector (kafka-connect-spooldir)
## Terminal 1 ("Kafka Connect")
docker compose exec connect confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:latest
docker compose restart connect
// verify the plugin is loaded
curl -s http://localhost:8083/connector-plugins | grep SpoolDir

## Terminal 2 ("ksqlDB cli")
// docker compose restart ksqldb-server
// if you edited docker-compose.yml:
// docker compose up -d --force-recreate ksqldb-server
docker compose exec ksqldb-server ksql http://localhost:8088
ksql> RUN SCRIPT '/ksql-scripts/file_connector_pipeline.sql';

### Prelim checks
SHOW CONNECTORS;
// or from terminal
curl -s http://localhost:8083/connectors/csv_file_source/status
// if status is FAILED
curl -s http://localhost:8083/connectors/CSV_FILE_SOURCE/status | jq
ksql> SET 'auto.offset.reset' = 'earliest';
ksql> PRINT 'US_hotel_reviews_csv' FROM BEGINNING LIMIT 5;

### Check raw incoming stream:
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM reviews_csv_raw EMIT CHANGES LIMIT 10;
SELECT COUNT(*) FROM reviews_csv_raw GROUP BY 1 EMIT CHANGES;
SELECT `id`, `name`, `city`, `reviews.title`, `reviews.username`, `reviews.rating` FROM reviews_csv_raw EMIT CHANGES LIMIT 10;
### Check filtered/cleaned silver stream:
SELECT hotel_id, hotel_name, postal_code, reviews_title, reviews_username, reviews_rating FROM reviews_csv_keyed EMIT CHANGES LIMIT 10;
### Query the aggregated materialized table:
SET 'auto.offset.reset' = 'latest';
SELECT * FROM hotel_reviews_stats 
WHERE total_reviews > 50 AND avg_rating > 3.0;

# Misc

## Clean CSV file by removing BOM (hidden char) in the id field
sed -i '1s/^\xef\xbb\xbf//' ./Hotel_Reviews.cs

## Grep any errors in log 
docker compose logs connect | grep -E "ERROR|EXCEPTION|SpoolDir" | tail -n 25

## Wipe the topic & re-process cleanly
docker compose exec connect kafka-topics --bootstrap-server broker:29092 --delete --topic US_hotel_reviews_csv

## Sourcing SQL scripts
## Your running background ksqldb-server container doesn't have your host file mounted.
## If you want your local SQL files to always be available inside the running ksqldb-server container, 
## mount the local directory in your docker-compose.yml file under the ksqldb-server service

## Docker
docker compose up -d
docker compose ps
docker compose down
docker compose down && docker compose up -d
## restart only the connect container
docker compose up -d --force-recreate connect
### Check for specific docker image
docker image ls | grep confluentinc
docker image ls confluentinc/cp-kafka-connect
docker image inspect confluentinc/cp-ksqldb-cli:8.2.0
### Remove a docker image
docker image rm confluentinc/cp-kafka-connect:latest